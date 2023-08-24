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

import java.io.Serializable;

public class LogReplayerImpl implements LogReplayer {

	private static final Logger LOG = LoggerFactory.getLogger(LogReplayer.class);

	private final DeterminantEncoder determinantEncoder;

	private final ByteBuf log;
	private final ByteBuf log_before;
	private final int CAUSAL_BUFFER_SIZE = 10485760; //リーダから受信するCausal Logのバッファサイズ (10 MB)

	// Use ReentrantLock for guaranteeing wait order
	private final Lock lock = new ReentrantLock(true); 
	private final Condition notEmpty = lock.newCondition();
	
	private final Thread tcpClient;
	private final BlockingQueue<ByteBuf> queue = new LinkedBlockingQueue<>(); //wait queue for leader causal logs
	private Thread waitCausalLog;
	final MeConfig meConfig = new MeConfig();

	private final DeterminantPool determinantPool;
	private final RecoveryManagerContext context;

	Determinant nextDeterminant;

	private boolean done;

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
		done = false;
	}

	@Override
	public synchronized int replayRandomInt() {
		LOG.debug("LogReplay replayRandomInt called.");
		deserializeNext();
		assert nextDeterminant instanceof RNGDeterminant;
		final RNGDeterminant rngDeterminant = (RNGDeterminant) nextDeterminant;
		//deserializeNext();
		int toReturn = rngDeterminant.getNumber();
		postHook(rngDeterminant);
		return toReturn;
	}

	@Override
	public synchronized byte replayNextChannel() {
		LOG.debug("LogReplay replayNextChannel called.");
		deserializeNext();
		assert nextDeterminant instanceof OrderDeterminant;
		final OrderDeterminant orderDeterminant = ((OrderDeterminant) nextDeterminant);
		//deserializeNext();
		byte toReturn = orderDeterminant.getChannel();
		postHook(orderDeterminant);
		return toReturn;
	}

	@Override
	public synchronized  long replayNextTimestamp() {
		LOG.debug("LogReplay replayNextTimestamp called.");
		deserializeNext();
		assert nextDeterminant instanceof TimestampDeterminant;
		final TimestampDeterminant timestampDeterminant = ((TimestampDeterminant) nextDeterminant);
		//deserializeNext();
		long toReturn = timestampDeterminant.getTimestamp();
		postHook(timestampDeterminant);
		return toReturn;
	}

	@Override
	public synchronized Object replaySerializableDeterminant() {
		LOG.debug("LogReplay replaySerializableDeterminant called.");
		deserializeNext();
		assert nextDeterminant instanceof SerializableDeterminant;
		final SerializableDeterminant serializableDeterminant = (SerializableDeterminant) nextDeterminant;
		//deserializeNext();
		Object toReturn = serializableDeterminant.getDeterminant();
		postHook(serializableDeterminant);
		return toReturn;
	}


	@Override
	public synchronized void triggerAsyncEvent() {
		LOG.debug("LogReplay triggerAsyncEvent called.");
		deserializeNext();
		assert nextDeterminant instanceof AsyncDeterminant;
		AsyncDeterminant asyncDeterminant = (AsyncDeterminant) nextDeterminant;
		int currentRecordCount = context.epochTracker.getRecordCount();

		if (LOG.isDebugEnabled())
			LOG.debug("Trigger {}", asyncDeterminant);

		if (currentRecordCount != asyncDeterminant.getRecordCount())
			throw new RuntimeException("Current record count is not the determinants record count. Current: " + currentRecordCount + ", determinant: " + asyncDeterminant.getRecordCount());

		// This async event might use another nondeterministic event in its callback, so we deserialize next first
		// deserializeNext();
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


	private void deserializeNext() {
		lock.lock();
		nextDeterminant = null;
		try {
			if (log != null && log.isReadable()) {
				while(log.isReadable()){
					short determinantVertexID = log.readShort();
					System.out.println("bytebuf vertex ID: " + determinantVertexID);
					if(determinantVertexID != context.vertexGraphInformation.getThisTasksVertexID().getVertexID()){
						determinantEncoder.decodeNext(log, determinantPool);
						if(!log.isReadable()){
							notEmpty.await();
						}
						continue;
					}
					System.out.println(determinantVertexID + " : " + log.toString());
					nextDeterminant = determinantEncoder.decodeNext(log, determinantPool);
					if (LOG.isDebugEnabled())
						LOG.debug("Deserialized nextDeterminant: {}", nextDeterminant);
					break;
				}
			}
			// NOT received Causal Log from leader node
			else{
				notEmpty.await(); // wait for leader causal log
				while(log.isReadable()){
					short determinantVertexID = log.readShort();
					if(determinantVertexID != context.vertexGraphInformation.getThisTasksVertexID().getVertexID()){
						determinantEncoder.decodeNext(log, determinantPool);
						if(!log.isReadable()){
							notEmpty.await();
						}
						continue;
					}
					System.out.println(determinantVertexID + " : " + log.toString());
					nextDeterminant = determinantEncoder.decodeNext(log, determinantPool);
					if (LOG.isDebugEnabled())
						LOG.debug("Deserialized nextDeterminant (await): {}", nextDeterminant);
					break;
				}
			}
		} catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }finally{
			lock.unlock();
		}
		if (nextDeterminant instanceof AsyncDeterminant)
			context.epochTracker.setRecordCountTarget(((AsyncDeterminant) nextDeterminant).getRecordCount());
	}

	private void postHook(Determinant determinant) {
		//deserializeNext();
		determinantPool.recycle(determinant);
		//if (nextDeterminant instanceof AsyncDeterminant)
		//	context.epochTracker.setRecordCountTarget(((AsyncDeterminant) nextDeterminant).getRecordCount());
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
					ByteBuf value = queue.take();
					//System.out.println(value.toString());
					/*
					short determinantVertexID = value.readShort();
					System.out.println("bytebuf vertex ID: " + determinantVertexID);
					if(determinantVertexID != context.vertexGraphInformation.getThisTasksVertexID().getVertexID()){
						value.release();
						continue;
					}
					*/
					lock.lock();
					try {
						log.writeBytes(value);
						notEmpty.signal(); // resume waiting thread
					} finally {
						lock.unlock();
					}
				}
			} catch (InterruptedException e) {
				System.out.println("TCPWriter interrupted!");
				//e.printStackTrace();
			}
		}
	}
}
