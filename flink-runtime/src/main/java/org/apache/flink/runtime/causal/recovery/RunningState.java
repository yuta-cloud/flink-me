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

import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.determinant.AsyncDeterminant;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * We either start in this state, or transition to it after the full recovery.
 */
public class RunningState extends AbstractState {

	private static final Logger LOG = LoggerFactory.getLogger(RunningState.class);

	public RunningState(RecoveryManager recoveryManager, RecoveryManagerContext context) {
		super(recoveryManager, context);
	}

	@Override
	public void executeEnter() {
		//context.processingTimeForceable.concludeReplay();

		for (AsyncDeterminant delayedRPCRequest : context.unansweredRPCRequests) {
			//TODO I think this may block? Or at least cause problems.Should probably execute these async
			logDebugWithVertexID("Executing delayed RPC request: {}", delayedRPCRequest);
			//delayedRPCRequest.setRecordCount(context.recordCounter.getRecordCount());
			//delayedRPCRequest.process(context);
		}
		context.unansweredRPCRequests.clear();
	}

	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		//Subpartitions might still be recovering
		PipelinedSubpartition subpartition = context.subpartitionTable.get(e.getIntermediateResultPartitionID(),
			e.getSubpartitionIndex());
		if (subpartition.isRecoveringSubpartititionInFlightState())
			super.notifyInFlightLogRequestEvent(e);
		else {
			logInfoWithVertexID("Received an InflightLogRequest {}", e);
			logDebugWithVertexID("intermediateResultPartition to request replay from: {}", e.getIntermediateResultPartitionID());
			subpartition.requestReplay(e.getCheckpointId(), e.getNumberOfBuffersToSkip());
		}
	}


	@Override
	public void notifyDeterminantRequestEvent(DeterminantRequestEvent e, int channelRequestArrivedFrom) {
		logInfoWithVertexID("Received a determinant request {} on channel {}", e, channelRequestArrivedFrom);
		//Since we are in running state, we can simply reply

		try {
			DeterminantResponseEvent responseEvent =
				context.causalLog.respondToDeterminantRequest(e);
			logInfoWithVertexID("Responding with: {}", responseEvent);

			context.inputGate.getInputChannel(channelRequestArrivedFrom).sendTaskEvent(responseEvent);
		} catch (IOException | InterruptedException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void notifyNewInputChannel(InputChannel inputChannel, int consumedSubpartitionIndex,
									  int numBuffersRemoved) {

		if (!(inputChannel instanceof RemoteInputChannel))
			return;
		if (context.causalLog.getDeterminantSharingDepth() == 0) {
			SingleInputGate singleInputGate = inputChannel.getInputGate();
			int channelIndex = inputChannel.getChannelIndex();

			context.invokable.resetInputChannelDeserializer(singleInputGate, channelIndex);

		}
	}

	@Override
	public String toString() {
		return "RunningState{}";
	}
}
