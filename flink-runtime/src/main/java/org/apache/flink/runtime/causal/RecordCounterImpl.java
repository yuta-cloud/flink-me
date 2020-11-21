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

package org.apache.flink.runtime.causal;


import org.apache.flink.runtime.causal.recovery.IRecoveryManager;

/**
 * All interaction with the record counter should be done inside the checkpoint lock.
 */
public final class RecordCounterImpl implements RecordCounter {

	public static final int NO_RECORD_COUNT_TARGET = -1;
	//The current input record count
	private int recordCount;

	//The input record count target at which the recovery manager should be notified.
	private int recordCountTarget;

	private IRecoveryManager recoveryManager;

	private boolean operatorsOpened;

	public RecordCounterImpl() {
		recordCount = 0;
		recordCountTarget = NO_RECORD_COUNT_TARGET;
		operatorsOpened = false;
	}

	@Override
	public final int getRecordCount() {
		return recordCount;
	}

	@Override
	public final void incRecordCount() {
		recordCount++;

		//Before returning control to the caller, check if we first should execute async nondeterministic event
		fireAnyAsyncEvent();
	}

	@Override
	public final void resetRecordCount() {
		recordCount = 0;
		//check if async event is first event of the epoch
		fireAnyAsyncEvent();
	}

	@Override
	public void setRecordCountTarget(int target) {
		this.recordCountTarget = target;
		//check if async event is first event of the first epoch
		fireAnyAsyncEvent();
	}

	@Override
	public void setRecoveryManager(IRecoveryManager recoveryManager) {
		this.recoveryManager = recoveryManager;
	}

	public void setOperatorsOpened(){
		operatorsOpened = true;
		fireAnyAsyncEvent();
	}

	private void fireAnyAsyncEvent() {
		if(operatorsOpened) {
			while (recordCountTarget == recordCount) {
				recordCountTarget = NO_RECORD_COUNT_TARGET;
				recoveryManager.triggerAsyncEvent();
			}
		}
	}
}
