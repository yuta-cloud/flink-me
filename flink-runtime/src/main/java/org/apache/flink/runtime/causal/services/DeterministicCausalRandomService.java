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

import org.apache.flink.api.common.services.RandomService;
import org.apache.flink.runtime.causal.determinant.RNGDeterminant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeterministicCausalRandomService extends AbstractCausalService implements RandomService {

	//Not thread safe
	protected final XORShiftRandom rng;

	//RNG determinant object used to avoid object creation and so garbage collection
	private final RNGDeterminant reuseRNGDeterminant;

	private static final Logger LOG = LoggerFactory.getLogger(DeterministicCausalRandomService.class);


	public DeterministicCausalRandomService(JobCausalLog jobCausalLog, IRecoveryManager recoveryManager) {
		super(jobCausalLog, recoveryManager);
		this.reuseRNGDeterminant = new RNGDeterminant();
		rng = new XORShiftRandom();
	}

	@Override
	public int nextInt() {

		return this.nextInt(Integer.MAX_VALUE);
	}

	@Override
	public int nextInt(int maxExclusive) {
		return rng.nextInt(maxExclusive);
	}

	private void updateSeed(){
		//Use rng determinant to record seed
		int seed;

		if (isRecovering()) {
			seed = recoveryManager.getLogReplayer().replayRandomInt();
			if (LOG.isDebugEnabled())
				LOG.debug("nextInt(): (State: RECOVERING) Replayed seed is {}", seed);
		} else {
			seed = (int) System.currentTimeMillis();
			threadCausalLog.appendDeterminant(reuseRNGDeterminant.replace(seed), epochTracker.getCurrentEpoch());
			if (LOG.isDebugEnabled())
				LOG.debug("nextInt(): (State: RUNNING) Fresh seed is {}", seed);
		}

		rng.setSeed(seed);
	}

	@Override
	public void notifyEpochStart(long epochID){
		updateSeed();
	}

}
