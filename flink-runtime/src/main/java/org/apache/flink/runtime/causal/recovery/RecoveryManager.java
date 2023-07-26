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

import org.apache.flink.runtime.causal.*;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.causal.recovery.MeConfig;


public class RecoveryManager implements IRecoveryManager {

	private static final Logger LOG = LoggerFactory.getLogger(RecoveryManager.class);

	public static final SinkRecoveryStrategy sinkRecoveryStrategy = SinkRecoveryStrategy.TRANSACTIONAL;

	public enum SinkRecoveryStrategy {
		TRANSACTIONAL,
		KAFKA
	}

	private State currentState;

	private final RecoveryManagerContext context;


	public RecoveryManager(RecoveryManagerContext context) {

		this.context = context;
		context.setOwner(this);

		//this.currentState = context.readyToReplayFuture == null ? new RunningState(this, context) :
		//	new StandbyState(this, context);
		MeConfig config = new MeConfig();
		this.currentState = config.isLeader() ? new RunningState(this, context) :
			new ReplayingState(this, context);
		LOG.info("Starting recovery manager in state {}", currentState);
	}

//====================== State Machine Messages ========================================

	@Override
	public synchronized void notifyStartRecovery() {
		this.currentState.notifyStartRecovery();
	}


	@Override
	public synchronized void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		this.currentState.notifyDeterminantResponseEvent(e);
	}

	@Override
	public synchronized void notifyDeterminantRequestEvent(DeterminantRequestEvent e, int channelRequestArrivedFrom) {
		this.currentState.notifyDeterminantRequestEvent(e, channelRequestArrivedFrom);
	}

	@Override
	public synchronized void notifyStateRestorationStart(long checkpointId) {
		this.currentState.notifyStateRestorationStart(checkpointId);
	}

	@Override
	public synchronized void notifyStateRestorationComplete(long checkpointId) {
		this.currentState.notifyStateRestorationComplete(checkpointId);
	}

	@Override
	public synchronized void notifyNewInputChannel(InputChannel inputChannel, int consumedSupartitionIndex,
												   int numberBuffersRemoved) {
		this.currentState.notifyNewInputChannel(inputChannel, consumedSupartitionIndex, numberBuffersRemoved);
	}

	@Override
	public synchronized void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID,
													int index) {
		this.currentState.notifyNewOutputChannel(intermediateResultPartitionID, index);
	}

	@Override
	public synchronized void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		this.currentState.notifyInFlightLogRequestEvent(e);
	}

	public synchronized void setState(State state) {
		this.currentState = state;
		//this.currentState.executeEnter();
	}

	//============== Check state ==========================
	@Override
	public synchronized boolean isRecovering() {
		return !(currentState instanceof RunningState);
	}

	@Override
	public synchronized boolean isReplaying() {
		return currentState instanceof ReplayingState;
	}

	@Override
	public synchronized boolean isRestoringState() {
		return !context.incompleteStateRestorations.isEmpty();
	}

	@Override
	public synchronized boolean isWaitingConnections() {
		return currentState instanceof WaitingConnectionsState;
	}

	@Override
	public synchronized RecoveryManagerContext getContext() {
		return context;
	}


	public State getState() {
		return currentState;
	}

	public LogReplayer getLogReplayer() {
		return currentState.getLogReplayer();
	}




}
