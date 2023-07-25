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
import org.apache.flink.runtime.causal.determinant.TimestampDeterminant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.runtime.causal.recovery.MeConfig;

public class CausalTimeService extends AbstractCausalService implements TimeService {

	//Timestamp determinant object used to avoid object creation and so garbage collection
	private final TimestampDeterminant reuseTimestampDeterminant;

	private final MeConfig config = new MeConfig();

	private static final Logger LOG = LoggerFactory.getLogger(CausalTimeService.class);

	public CausalTimeService(JobCausalLog causalLoggingManager, IRecoveryManager recoveryManager) {
		super(causalLoggingManager, recoveryManager);
		this.reuseTimestampDeterminant = new TimestampDeterminant();
	}

	@Override
	public long currentTimeMillis() {
		long toReturn;

		if (isRecovering()) {
		//if(!config.isLeader()){
			toReturn = recoveryManager.getLogReplayer().replayNextTimestamp();
			if (LOG.isDebugEnabled())
				LOG.info("currentTimeMillis(): (State: RECOVERING) Replayed timestamp is {}", toReturn);
		} else {
			toReturn = System.currentTimeMillis();
			if (LOG.isDebugEnabled())
				LOG.info("currentTimeMillis(): (State: RUNNING) Fresh timestamp is {}", toReturn);
		}

		//Whether we are recovering or not, we append the determinant. If recovering, we still need to restore the
		// causal log to the pre-failure state.
		threadCausalLog.appendDeterminant(reuseTimestampDeterminant.replace(toReturn),
			epochTracker.getCurrentEpoch());

		return toReturn;
	}
}
