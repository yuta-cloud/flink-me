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

import com.google.common.collect.Table;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.io.IOException;

/**
 * In this state we do the actual process of recovery. Once done transition to {@link RunningState}
 * <p>
 * A downstream failure in this state does not matter, requests simply get added to the queue of unanswered requests.
 * Only when we finish our recovery do we answer those requests.
 * <p>
 * Upstream failures in this state mean that perhaps the upstream will not have sent all buffers. This means we must
 * resend
 * replay requests.
 */
public class ReplayingState extends AbstractState {

	private static final Logger LOG = LoggerFactory.getLogger(ReplayingState.class);

	private final LogReplayer logReplayer;

	public ReplayingState(RecoveryManager recoveryManager, RecoveryManagerContext context,
						  DeterminantResponseEvent determinantAccumulator) {
		super(recoveryManager, context);
		logInfoWithVertexID("Entered replaying state with determinants: {}", determinantAccumulator);

		ByteBuf log =
			determinantAccumulator.getDeterminants().get(new CausalLogID(context.getTaskVertexID()));
		logReplayer = new LogReplayerImpl(log, context);
		createSubpartitionRecoveryThreads(determinantAccumulator);
	}

	public void executeEnter() {
		logReplayer.checkFinished();
		context.readyToReplayFuture.complete(null);//allow task to start running
	}

	@Override
	public void notifyNewInputChannel(InputChannel inputChannel, int consumedSubpartitionIndex,
									  int numberOfBuffersRemoved) {
		//we got notified of a new input channel while we were replaying
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//Furthermore, we have to resend the inflight log request, and ask to skip X buffers

		logInfoWithVertexID("Got notified of new input channel event, while in state " + this.getClass()
			+ " requesting upstream to replay and skip numberOfBuffersRemoved");
		if (!(inputChannel instanceof RemoteInputChannel))
			return;
		IntermediateResultPartitionID id = inputChannel.getPartitionId().getPartitionId();
		try {
			inputChannel.sendTaskEvent(new InFlightLogRequestEvent(id, consumedSubpartitionIndex,
				context.getEpochTracker().getCurrentEpoch(), numberOfBuffersRemoved));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public LogReplayer getLogReplayer() {
		return logReplayer;
	}

	@Override
	public String toString() {
		return "ReplayingState{}";
	}

	private void createSubpartitionRecoveryThreads(DeterminantResponseEvent determinantResponseEvent) {

		CausalLogID id = new CausalLogID(context.vertexGraphInformation.getThisTasksVertexID().getVertexID());

		for (Table.Cell<IntermediateResultPartitionID, Integer, PipelinedSubpartition> cell :
			context.subpartitionTable.cellSet()) {

			PipelinedSubpartition subpartition = cell.getValue();
			IntermediateResultPartitionID partitionID = cell.getRowKey();
			byte index = cell.getColumnKey().byteValue();

			id.replace(partitionID.getLowerPart(), partitionID.getUpperPart(), index);

			ByteBuf subpartitionBuf = determinantResponseEvent.getDeterminants().get(id);
			ByteBuf recoveryBuffer = Unpooled.EMPTY_BUFFER;
			if (subpartitionBuf != null)
				recoveryBuffer = subpartitionBuf;

			Thread t = new SubpartitionRecoveryThread(recoveryBuffer, subpartition, context, partitionID,
				index);
			t.setDaemon(true);
			t.start();

			logDebugWithVertexID("Created recovery thread for Partition {} subpartition index {} with buffer {}", cell.getRowKey(),
				cell.getColumnKey(), recoveryBuffer);
		}
	}

	private static class SubpartitionRecoveryThread extends Thread {
		private final PipelinedSubpartition pipelinedSubpartition;
		private final ByteBuf recoveryBuffer;
		private final DeterminantEncoder determinantEncoder;
		private final RecoveryManagerContext context;
		private final IntermediateResultPartitionID partitionID;
		private final int index;

		public SubpartitionRecoveryThread(ByteBuf recoveryBuffer, PipelinedSubpartition pipelinedSubpartition,
										  RecoveryManagerContext context, IntermediateResultPartitionID partitionID,
										  int index) {
			this.recoveryBuffer = recoveryBuffer;
			this.pipelinedSubpartition = pipelinedSubpartition;
			this.determinantEncoder = context.causalLog.getDeterminantEncoder();
			this.context = context;
			this.partitionID = partitionID;
			this.index = index;

		}

		@Override
		public void run() {
			//1. Netty has been told that there is no data.
			DeterminantPool determinantPool = new DeterminantPool();

			//2. Rebuild in-flight log and subpartition state
			Determinant determinant;
			while (recoveryBuffer.isReadable()) {
				try {
					determinant = determinantEncoder.decodeNext(recoveryBuffer, determinantPool);
				} catch (Exception e) {
					LOG.error("Vertex {} - Recovery thread for partition {} index {} found exception",
						context.getTaskVertexID(), partitionID, index, e);
					throw e;
				}

				if (!(determinant instanceof BufferBuiltDeterminant))
					throw new RuntimeException("Vertex " + context.getTaskVertexID() + " - " +
						"Subpartition has corrupt recovery buffer, expected buffer built," +
						" " +
						"got: "
						+ determinant);
				BufferBuiltDeterminant bufferBuiltDeterminant = (BufferBuiltDeterminant) determinant;

				if(LOG.isDebugEnabled())
					LOG.debug("Vertex {} - Requesting to build and log buffer with {} bytes",
					context.getTaskVertexID(),
					bufferBuiltDeterminant.getNumberOfBytes());
				try {
					pipelinedSubpartition.buildAndLogBuffer(bufferBuiltDeterminant.getNumberOfBytes());
				} catch (InterruptedException e) {
					return;
				}
				determinantPool.recycle(determinant);
			}
			LOG.info("Vertex {} - Done recovering pipelined subpartition", context.getTaskVertexID());
			//Safety check that recovery brought us to the exact same state as pre-failure
			int logLengthAfterRecovery =
				context.causalLog.threadLogLength(new CausalLogID(context.getTaskVertexID(),
					partitionID.getLowerPart(), partitionID.getUpperPart(), (byte) index));
			assert recoveryBuffer.capacity() == logLengthAfterRecovery;

			// If there is a replay request, we have to prepare it, before setting isRecovering to true
			InFlightLogRequestEvent unansweredRequest =
				context.unansweredInFlightLogRequests.remove(partitionID, index);
			LOG.debug("Checking for unanswered inflight request for this subpartition.");
			if (unansweredRequest != null) {
				LOG.info("There is an unanswered replay request for this subpartition.");
				pipelinedSubpartition.requestReplay(unansweredRequest.getCheckpointId(),
					unansweredRequest.getNumberOfBuffersToSkip());
			}

			//3. Tell netty to restart requesting buffers.
			pipelinedSubpartition.setIsRecoveringSubpartitionInFlightState(false);
			pipelinedSubpartition.notifyDataAvailable();
			recoveryBuffer.release();
			LOG.debug("Subpartition is free to restart sending buffers.");

		}
	}

}
