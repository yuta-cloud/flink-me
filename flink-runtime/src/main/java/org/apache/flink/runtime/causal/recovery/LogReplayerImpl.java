/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.causal.recovery;

import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.io.Serializable;

public class LogReplayerImpl implements LogReplayer {

	private static final Logger LOG = LoggerFactory.getLogger(LogReplayer.class);

	private final DeterminantEncoder determinantEncoder;

	private final ByteBuf log;
	private final ByteBuf log_before;
	private final int CAUSAL_BUFFER_SIZE = 104857600; //リーダから受信するCausal Logのバッファサイズ (10 MB)
	private final int TIMEOUT = 200;
	private final int FIRST_READ = 200;
	private int firstRead = 0;

	// Use ReentrantLock for guaranteeing wait order
	private final Lock lock = new ReentrantLock(); 
	private final Condition notEmpty = lock.newCondition();
	
	private final Thread tcpClient;
	private final BlockingQueue<ByteBuf> queue = new LinkedBlockingQueue<>(); //wait queue for leader causal logs
	private Thread waitCausalLog;
	final MeConfig meConfig = new MeConfig();

	private final DeterminantPool determinantPool;
	private final RecoveryManagerContext context;

	Determinant nextDeterminant;

	private boolean done;
	private boolean checkFlag;

	public LogReplayerImpl(ByteBuf log, RecoveryManagerContext recoveryManagerContext) {
		this.context = recoveryManagerContext;
		this.determinantEncoder = context.causalLog.getDeterminantEncoder();
		this.log_before = log;
		this.log = Unpooled.buffer(CAUSAL_BUFFER_SIZE);
		this.determinantPool = new DeterminantPool();
		waitCausalLog = new Thread(() -> {new WaitCausalLog().run();});
		tcpClient = new Thread(() -> {
            new MeTCPClient(queue, meConfig).run();
        });
		waitCausalLog.start();
		tcpClient.start();
		deserializeNext(true);
		done = false;
		checkFlag = false;
	}

	@Override
	public synchronized int replayRandomInt() {
		LOG.debug("LogReplay replayRandomInt called.");
		//deserializeNext(true);
		assert nextDeterminant instanceof RNGDeterminant;
		final RNGDeterminant rngDeterminant = (RNGDeterminant) nextDeterminant;
		deserializeNext(true);
		int toReturn = rngDeterminant.getNumber();
		postHook(rngDeterminant);
		return toReturn;
	}

	@Override
	public synchronized byte replayNextChannel() {
		LOG.debug("LogReplay replayNextChannel called.");
		//deserializeNext(true);
		assert nextDeterminant instanceof OrderDeterminant;
		final OrderDeterminant orderDeterminant = ((OrderDeterminant) nextDeterminant);
		deserializeNext(true);
		byte toReturn = orderDeterminant.getChannel();
		postHook(orderDeterminant);
		return toReturn;
	}

	@Override
	public synchronized  long replayNextTimestamp() {
		LOG.debug("LogReplay replayNextTimestamp called.");
		//deserializeNext(true);
		//TimerTriggerDeterminant may be appended before startNewEpoch called
		if (nextDeterminant instanceof AsyncDeterminant)
			context.epochTracker.setRecordCountTarget(((AsyncDeterminant) nextDeterminant).getRecordCount());
		assert nextDeterminant instanceof TimestampDeterminant;
		final TimestampDeterminant timestampDeterminant = ((TimestampDeterminant) nextDeterminant);
		deserializeNext(true);
		long toReturn = timestampDeterminant.getTimestamp();
		postHook(timestampDeterminant);
		return toReturn;
	}

	@Override
	public synchronized Object replaySerializableDeterminant() {
		LOG.debug("LogReplay replaySerializableDeterminant called.");
		//deserializeNext();
		assert nextDeterminant instanceof SerializableDeterminant;
		final SerializableDeterminant serializableDeterminant = (SerializableDeterminant) nextDeterminant;
		deserializeNext(true);
		Object toReturn = serializableDeterminant.getDeterminant();
		postHook(serializableDeterminant);
		return toReturn;
	}


	@Override
	public synchronized void triggerAsyncEvent() {
		LOG.debug("LogReplay triggerAsyncEvent called.");
		assert nextDeterminant instanceof AsyncDeterminant;
		AsyncDeterminant asyncDeterminant = (AsyncDeterminant) nextDeterminant;
		int currentRecordCount = context.epochTracker.getRecordCount();

		if (LOG.isDebugEnabled())
			LOG.debug("Trigger {}", asyncDeterminant);

		if (currentRecordCount != asyncDeterminant.getRecordCount())
			throw new RuntimeException("Current record count is not the determinants record count. Current: " + currentRecordCount + ", determinant: " + asyncDeterminant.getRecordCount());

		// This async event might use another nondeterministic event in its callback, so we deserialize next first
		deserializeNext(false);
		//Then we process the actual event
		asyncDeterminant.process(context);
		//Only then can we actually set the next target, possibly triggering another async event of the same record count.
		postHook(asyncDeterminant);
	}

	public synchronized void checkFinished() {
		if (!done) {
			if (isFinished()) {
				if (log != null) {
					done = true;
					//Safety check that recovery brought us to the exact same causal log state as pre-failure
					assert log.capacity() ==
						context.causalLog.threadLogLength(new CausalLogID(context.getTaskVertexID()));
					log.release();
				}
				LOG.info("Finished recovering main thread! Transitioning to RunningState!");
				context.owner.setState(new RunningState(context.owner, context));
			}
		}
	}

	public void checkFinishedMe() {
		LOG.info("Enter checkFinishedMe");
		lock.lock();
		try {
			checkFlag = true;
			notEmpty.signalAll(); // resume waiting thread
		} finally {
			lock.unlock();
		}
		LOG.debug("checkFinished() signal");
		if (!done) {
			done = true;
			//Safety check that recovery brought us to the exact same causal log state as pre-failure
			log.release();
			LOG.info("Finished recovering main thread! Transitioning to RunningState!");
			context.owner.setState(new RunningState(context.owner, context));
			LOG.info("Finished-2 set RunningState!");
		}
	}

	@Override
	public void deserializeNext(boolean check) {
		lock.lock();
		nextDeterminant = null;
		try {
			if (log != null && log.isReadable()) {
				LOG.debug("LOCK wait now1!");
				while(log.isReadable() && !checkFlag){
					short determinantVertexID = log.readShort();
					if(determinantVertexID < 0){
						break;
					}
					//System.out.println("bytebuf vertex ID: " + determinantVertexID);
					if(determinantVertexID != context.vertexGraphInformation.getThisTasksVertexID().getVertexID()){
						determinantEncoder.decodeNext(log, determinantPool);
						if(!log.isReadable()){
							notEmpty.await();
						}
						continue;
					}
					//(determinantVertexID + " : " + log.toString());
					nextDeterminant = determinantEncoder.decodeNext(log, determinantPool);
					if (LOG.isDebugEnabled())
						LOG.debug("Deserialized nextDeterminant: {}, {}", determinantVertexID, nextDeterminant);
					break;
				}
			}
			// NOT received Causal Log from leader node
			else{
				LOG.debug("LOCK wait now2!");
				notEmpty.await(); // wait for leader causal log
				LOG.debug("LOCK wait notify!");
				while(log.isReadable() && !checkFlag){
					short determinantVertexID = log.readShort();
					// System.out.println("bytebuf vertex ID: " + determinantVertexID);
					if(determinantVertexID < 0){
						break;
					}
					if(determinantVertexID != context.vertexGraphInformation.getThisTasksVertexID().getVertexID()){
						determinantEncoder.decodeNext(log, determinantPool);
						if(!log.isReadable()){
							notEmpty.await();
						}
						continue;
					}
					//System.out.println(determinantVertexID + " : " + log.toString());
					nextDeterminant = determinantEncoder.decodeNext(log, determinantPool);
					if (LOG.isDebugEnabled())
						LOG.debug("Deserialized nextDeterminant (await): {}, {}", determinantVertexID, nextDeterminant);
					break;
				}
			}
		} catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }finally{
			lock.unlock();
			LOG.debug("UNLOCK now!");
		}
	}

	private void postHook(Determinant determinant) {
		//deserializeNext();
		determinantPool.recycle(determinant);
		if (nextDeterminant instanceof AsyncDeterminant)
			context.epochTracker.setRecordCountTarget(((AsyncDeterminant) nextDeterminant).getRecordCount());
		//checkFinished();
	}

	private boolean isFinished() {
		return nextDeterminant == null;
	}

	public class WaitCausalLog implements Runnable{
		@Override
		public void run() {
			System.out.println("WaitCausalLog start");
			try {
				while (true) {
					ByteBuf value;
					if(firstRead < FIRST_READ){
						value = queue.take();
						firstRead += 1;
					}else{
						value = queue.poll(TIMEOUT, TimeUnit.MILLISECONDS);
					}
					//System.out.println(value.toString());
					/*
					short determinantVertexID = value.readShort();
					System.out.println("bytebuf vertex ID: " + determinantVertexID);
					if(determinantVertexID != context.vertexGraphInformation.getThisTasksVertexID().getVertexID()){
						value.release();
						continue;
					}
					*/

					//リーダがタイムアウトした場合
					if(value == null){
						LOG.info("Change status to RUNNING bacause of timeout");
						checkFinishedMe();
						break;
					}

					lock.lock();
					try {
						log.writeBytes(value);
						notEmpty.signal(); // resume waiting thread
					} finally {
						lock.unlock();
						value.release();
					}
				}
			} catch (InterruptedException e) {
				System.out.println("TCPWriter interrupted!");
				//e.printStackTrace();
			}
		}
	}
}
