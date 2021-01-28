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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class LogReplayerImpl implements LogReplayer {

	private static final Logger LOG = LoggerFactory.getLogger(LogReplayer.class);

	private final DeterminantEncoder determinantEncoder;

	private final ByteBuf log;

	private final DeterminantPool determinantPool;
	private final RecoveryManagerContext context;

	Determinant nextDeterminant;

	public LogReplayerImpl(ByteBuf log, RecoveryManagerContext recoveryManagerContext) {
		this.context = recoveryManagerContext;
		this.determinantEncoder = context.causalLog.getDeterminantEncoder();
		this.log = log;
		this.determinantPool = new DeterminantPool();
		deserializeNext();
	}

	@Override
	public int replayRandomInt() {
		assert nextDeterminant instanceof RNGDeterminant;
		final RNGDeterminant rngDeterminant = (RNGDeterminant) nextDeterminant;
		deserializeNext();
		int toReturn = rngDeterminant.getNumber();
		postHook(rngDeterminant);
		return toReturn;
	}

	@Override
	public byte replayNextChannel() {
		assert nextDeterminant instanceof OrderDeterminant;
		final OrderDeterminant orderDeterminant = ((OrderDeterminant) nextDeterminant);
		deserializeNext();
		byte toReturn = orderDeterminant.getChannel();
		postHook(orderDeterminant);
		return toReturn;
	}

	@Override
	public long replayNextTimestamp() {
		assert nextDeterminant instanceof TimestampDeterminant;
		final TimestampDeterminant timestampDeterminant = ((TimestampDeterminant) nextDeterminant);
		deserializeNext();
		long toReturn = timestampDeterminant.getTimestamp();
		postHook(timestampDeterminant);
		return toReturn;
	}

	@Override
	public Object replaySerializableDeterminant() {
		assert nextDeterminant instanceof SerializableDeterminant;
		final SerializableDeterminant serializableDeterminant = (SerializableDeterminant) nextDeterminant;
		deserializeNext();
		Object toReturn = serializableDeterminant.getDeterminant();
		postHook(serializableDeterminant);
		return toReturn;
	}


	@Override
	public void triggerAsyncEvent() {
		assert nextDeterminant instanceof AsyncDeterminant;
		AsyncDeterminant asyncDeterminant = (AsyncDeterminant) nextDeterminant;
		int currentRecordCount = context.epochTracker.getRecordCount();

		if (LOG.isDebugEnabled())
			LOG.debug("Trigger {}", asyncDeterminant);

		if (currentRecordCount != asyncDeterminant.getRecordCount())
			throw new RuntimeException("Current record count is not the determinants record count. Current: " + currentRecordCount + ", determinant: " + asyncDeterminant.getRecordCount());

		// This async event might use another nondeterministic event in its callback, so we deserialize next first
		deserializeNext();
		//Then we process the actual event
		asyncDeterminant.process(context);
		//Only then can we actually set the next target, possibly triggering another async event of the same record count.
		postHook(asyncDeterminant);
	}

	public void checkFinished() {
		if (isFinished()) {
			if (log != null) {
				//Safety check that recovery brought us to the exact same causal log state as pre-failure
				assert log.capacity() ==
					context.causalLog.threadLogLength(new CausalLogID(context.getTaskVertexID()));
				log.release();
			}
			LOG.info("Finished recovering main thread! Transitioning to RunningState!");
			context.owner.setState(new RunningState(context.owner, context));
		}
	}


	private void deserializeNext() {
		nextDeterminant = null;
		if (log.isReadable())
			nextDeterminant = determinantEncoder.decodeNext(log, determinantPool);
	}

	private void postHook(Determinant determinant) {
		determinantPool.recycle(determinant);
		if (nextDeterminant instanceof AsyncDeterminant)
			context.epochTracker.setRecordCountTarget(((AsyncDeterminant) nextDeterminant).getRecordCount());
		checkFinished();
	}

	private boolean isFinished() {
		return nextDeterminant == null;
	}

}
