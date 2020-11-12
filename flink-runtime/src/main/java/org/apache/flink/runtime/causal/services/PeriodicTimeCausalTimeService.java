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

package org.apache.flink.runtime.causal.services;

import org.apache.flink.api.common.services.TimeService;
import org.apache.flink.runtime.causal.EpochProvider;
import org.apache.flink.runtime.causal.determinant.TimestampDeterminant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeriodicTimeCausalTimeService extends AbstractCausalService implements TimeService {
	//Timestamp determinant object used to avoid object creation and so garbage collection
	private final TimestampDeterminant reuseTimestampDeterminant;

	private static final Logger LOG = LoggerFactory.getLogger(PeriodicTimeCausalTimeService.class);
	private final long interval;

	//We need to use an array to store the current time, because Long is immutable and long is passed by value
	private long[] currentTime;


	public PeriodicTimeCausalTimeService(JobCausalLog causalLoggingManager, IRecoveryManager recoveryManager,
							 EpochProvider epochProvider, long interval) {
		super(causalLoggingManager, recoveryManager, epochProvider);
		this.reuseTimestampDeterminant = new TimestampDeterminant();
		this.interval = interval;
		this.currentTime = new long[]{Long.MIN_VALUE};
	}

	@Override
	public long currentTimeMillis() {

		if(LOG.isDebugEnabled())
			LOG.debug("Time requested. Returning {}.", currentTime[0]);
		return currentTime[0];
	}

	public long[] getCurrentTime() {
		return currentTime;
	}

	public long getInterval(){
		return interval;
	}

	public void notifyNewEpoch(){
		readOrWriteTimestampDeterminant();
	}

	void readOrWriteTimestampDeterminant(){
		//record timestamp in causal log
		if (isRecovering()) {
			currentTime[0] = recoveryManager.replayNextTimestamp();
			if (LOG.isDebugEnabled())
				LOG.debug("readOrWriteTimestamp: (State: RECOVERING) restored {}", currentTime[0]);
		} else {
			threadCausalLog.appendDeterminant(reuseTimestampDeterminant.replace(currentTime[0]), epochProvider.getCurrentEpochID());
			if (LOG.isDebugEnabled())
				LOG.debug("readOrWriteTimestamp(): (State: RUNNING) recorded {}", currentTime[0]);
		}
	}

}
