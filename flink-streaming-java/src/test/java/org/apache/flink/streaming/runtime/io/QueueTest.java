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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.causal.*;
import org.apache.flink.runtime.causal.determinant.AsyncDeterminant;
import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.determinant.DeterminantEncoder;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.causal.recovery.*;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class QueueTest {


	@Test
	public void testQueue() throws Exception {
		CausalBufferOrderService bos = new CausalBufferOrderService(new JCL(), new RM(), new CBH(), 3);


		BufferOrEvent one = bos.getNextBuffer();
		assert (one.getChannelIndex() == 1 && one.getBuffer().asByteBuf().readInt() == 12);
		BufferOrEvent two = bos.getNextBuffer();
		assert (two.getChannelIndex() == 2 && two.getBuffer().asByteBuf().readInt() == 1);
		BufferOrEvent three = bos.getNextBuffer();
		assert (three.getChannelIndex() == 2 && three.getBuffer().asByteBuf().readInt() == 3);

	}

	static class CBH implements CheckpointBarrierHandler {
		List<BufferOrEvent> list;
		NetworkBufferPool nbp = new NetworkBufferPool(100, 32768);
		BufferPool bp;

		private static int counter = 0;

		public CBH() throws IOException {
			bp = nbp.createBufferPool(100, 100);
			list = new LinkedList<>();
			list.add(mkBoe(0));
			list.add(mkBoe(2));
			list.add(mkBoe(0));
			list.add(mkBoe(2));
			list.add(mkBoe(0));
			list.add(mkBoe(2));

			list.add(mkBoe(0));
			list.add(mkBoe(0));
			list.add(mkBoe(0));
			list.add(mkBoe(0));
			list.add(mkBoe(0));
			list.add(mkBoe(0));

			list.add(mkBoe(1));


		}

		private BufferOrEvent mkBoe(int channel) throws IOException {
			Buffer b = bp.requestBuffer();
			b.asByteBuf().writeInt(counter++);
			return new BufferOrEvent(b, channel);

		}

		@Override
		public BufferOrEvent getNextNonBlocked() throws Exception {
			return list.remove(0);
		}

		@Override
		public void registerCheckpointEventHandler(AbstractInvokable task) {

		}

		@Override
		public void cleanup() throws IOException {

		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public long getAlignmentDurationNanos() {
			return 0;
		}

		@Override
		public void ignoreCheckpoint(long checkpointID) throws IOException {

		}

		@Override
		public void unblockChannelIfBlocked(int absoluteChannelIndex) {

		}
	}

	class LR implements LogReplayer {

		public List<Byte> channelReplay;

		public LR() {
			channelReplay = new LinkedList<>();
			channelReplay.add((byte) 1);
			channelReplay.add((byte) 2);
			channelReplay.add((byte) 2);


		}

		@Override
		public void triggerAsyncEvent() {

		}

		@Override
		public int replayRandomInt() {
			return 0;
		}

		@Override
		public byte replayNextChannel() {
			return channelReplay.remove(0);
		}

		@Override
		public long replayNextTimestamp() {
			return 0;
		}

		@Override
		public void checkFinished() {

		}

		@Override
		public Object replaySerializableDeterminant() {
			return null;
		}

		@Override
		public void deserializeNext(boolean check){

		}
	}

	class RM implements IRecoveryManager {
		private LogReplayer lr = new LR();

		@Override
		public void notifyNewInputChannel(InputChannel inputChannel, int consumedSupartitionIndex, int numberBuffersRemoved) {

		}

		@Override
		public void notifyNewOutputChannel(IntermediateResultPartitionID partitionId, int index) {

		}

		@Override
		public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {

		}

		@Override
		public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {

		}

		@Override
		public void notifyDeterminantRequestEvent(DeterminantRequestEvent e, int channelRequestArrivedFrom) {

		}

		@Override
		public void notifyStateRestorationStart(long checkpointId) {

		}

		@Override
		public void notifyStateRestorationComplete(long checkpointId) {

		}

		@Override
		public void notifyStartRecovery() {

		}

		@Override
		public boolean isRecovering() {
			return true;
		}

		@Override
		public boolean isReplaying() {
			return false;
		}

		@Override
		public boolean isRestoringState() {
			return false;
		}

		@Override
		public boolean isWaitingConnections() {
			return false;
		}

		@Override
		public IRecoveryManagerContext getContext() {
			return new IRecoveryManagerContext() {
				@Override
				public void setOwner(RecoveryManager owner) {

				}

				@Override
				public void setProcessingTimeService(ProcessingTimeForceable processingTimeForceable) {

				}

				@Override
				public ProcessingTimeForceable getProcessingTimeForceable() {
					return null;
				}

				@Override
				public CheckpointForceable getCheckpointForceable() {
					return null;
				}

				@Override
				public short getTaskVertexID() {
					return 0;
				}

				@Override
				public EpochTracker getEpochTracker() {
					return new EpochTrackerImpl();
				}

				@Override
				public void setInputGate(InputGate inputGate) {

				}

				@Override
				public void appendRPCRequestDuringRecovery(AsyncDeterminant determinant) {

				}

				@Override
				public int getNumberOfDirectDownstreamNeighbourVertexes() {
					return 0;
				}
			};
		}

		@Override
		public LogReplayer getLogReplayer() {
			return lr;
		}
	}

	class JCL implements JobCausalLog {

		@Override
		public void registerTask(VertexGraphInformation vertexGraphInformation, JobVertexID jobVertexId, ResultPartitionWriter[] resultPartitionsOfLocalVertex) {

		}

		@Override
		public ThreadCausalLog getThreadCausalLog(CausalLogID causalLogID) {
			return new ThreadCausalLog() {
				@Override
				public CausalLogID getCausalLogID() {
					return null;
				}

				@Override
				public ByteBuf getDeterminants(long startEpochID) {
					return null;
				}

				@Override
				public int logLength() {
					return 0;
				}

				@Override
				public void processUpstreamDelta(ByteBuf delta, int offsetFromEpoch, long epochID) {

				}

				@Override
				public void appendDeterminant(Determinant determinant, long epochID) {

				}

				@Override
				public boolean hasDeltaForConsumer(InputChannelID outputChannelID, long epochID) {
					return false;
				}

				@Override
				public int getOffsetFromEpochForConsumer(InputChannelID outputChannelID, long epochID) {
					return 0;
				}

				@Override
				public ByteBuf getDeltaForConsumer(InputChannelID outputChannelID, long epochID) {
					return null;
				}

				@Override
				public void notifyCheckpointComplete(long checkpointID) {

				}

				@Override
				public void close() {

				}

				@Override
				public void unregisterConsumer(InputChannelID toCancel) {

				}
			};
		}

		@Override
		public void processCausalLogDelta(ByteBuf msg) {

		}

		@Override
		public ByteBuf enrichWithCausalLogDelta(ByteBuf serialized, InputChannelID inputChannelID, long epochID, ByteBufAllocator alloc) {
			return null;
		}

		@Override
		public DeterminantResponseEvent respondToDeterminantRequest(DeterminantRequestEvent e) {
			return null;
		}

		@Override
		public void registerDownstreamConsumer(InputChannelID inputChannelID, CausalLogID consumedSubpartition) {

		}

		@Override
		public void unregisterDownstreamConsumer(InputChannelID toCancel) {

		}

		@Override
		public int threadLogLength(CausalLogID causalLogID) {
			return 0;
		}

		@Override
		public boolean unregisterTask(JobVertexID jobVertexId) {
			return false;
		}

		@Override
		public DeterminantEncoder getDeterminantEncoder() {
			return null;
		}

		@Override
		public int getDeterminantSharingDepth() {
			return 0;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {

		}
	}
}
