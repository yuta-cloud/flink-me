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

import org.apache.flink.api.common.services.SerializableService;
import org.apache.flink.runtime.causal.determinant.SerializableDeterminant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Function;

public class SerializableCausalService<I,O extends Serializable> extends AbstractCausalService implements SerializableService<I,O> {

	private static final Logger LOG = LoggerFactory.getLogger(SerializableCausalService.class);

	private final Function<I, O> f;
	private final SerializableDeterminant reuse;

	public SerializableCausalService(JobCausalLog causalLog, IRecoveryManager recoveryManager, Function<I,O> f) {
		super(causalLog, recoveryManager);
		this.f = f;
		this.reuse = new SerializableDeterminant();
	}

	@Override
	public O apply(I i) {
		O result;

		if(isRecovering()) {
			result = (O) recoveryManager.getLogReplayer().replaySerializableDeterminant();
			if(LOG.isDebugEnabled())
				LOG.debug("state: RECOVERING - Restored {}", result);
		}else {
			result = f.apply(i);
			if(LOG.isDebugEnabled())
				LOG.debug("state: RUNNING - Created {}", result);
		}

		threadCausalLog.appendDeterminant(reuse.replace(result), epochTracker.getCurrentEpoch());

		return result;
	}
}
