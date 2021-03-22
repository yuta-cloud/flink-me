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

import org.apache.flink.runtime.causal.CheckpointForceable;
import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.EpochTracker;
import org.apache.flink.runtime.causal.ProcessingTimeForceable;
import org.apache.flink.runtime.causal.determinant.AsyncDeterminant;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

public interface IRecoveryManagerContext {
	void setOwner(RecoveryManager owner);

	void setProcessingTimeService(ProcessingTimeForceable processingTimeForceable);

	ProcessingTimeForceable getProcessingTimeForceable();

	CheckpointForceable getCheckpointForceable();

	short getTaskVertexID();

	EpochTracker getEpochTracker();

	void setInputGate(InputGate inputGate);

	void appendRPCRequestDuringRecovery(AsyncDeterminant determinant);

	int getNumberOfDirectDownstreamNeighbourVertexes();

	public static class UnansweredDeterminantRequest {
		private int numResponsesReceived;
		private final int requestingChannel;

		private final DeterminantResponseEvent response;

		public UnansweredDeterminantRequest(DeterminantRequestEvent event, int requestingChannel) {
			this.numResponsesReceived = 0;
			this.requestingChannel = requestingChannel;
			this.response = new DeterminantResponseEvent(event);
			this.response.setCorrelationID(event.getUpstreamCorrelationID());
		}

		public int getNumResponsesReceived() {
			return numResponsesReceived;
		}


		public int getRequestingChannel() {
			return requestingChannel;
		}

		public void incResponsesReceived() {
			numResponsesReceived++;
		}

		public DeterminantResponseEvent getCurrentResponse() {
			return response;
		}

	}
}
