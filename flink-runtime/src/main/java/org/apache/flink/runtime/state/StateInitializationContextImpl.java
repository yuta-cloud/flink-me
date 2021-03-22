/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.services.*;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.util.CloseableIterable;

/**
 * Default implementation of {@link StateInitializationContext}.
 */
public class StateInitializationContextImpl implements StateInitializationContext {

	/**
	 * Signal whether any state to restore was found
	 */
	private final boolean restored;
	private final boolean isStandby;

	private final OperatorStateStore operatorStateStore;

	private final KeyedStateStore keyedStateStore;

	private final Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;
	private final Iterable<StatePartitionStreamProvider> rawOperatorStateInputs;
	private final SerializableServiceFactory serializableServiceFactory;
	private final RandomService randomService;
	private final TimeService timestampService;

	public StateInitializationContextImpl(
		boolean restored,
		boolean isStandby,
		OperatorStateStore operatorStateStore,
		KeyedStateStore keyedStateStore,
		Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs,
		Iterable<StatePartitionStreamProvider> rawOperatorStateInputs) {
		this(restored, isStandby, operatorStateStore, keyedStateStore, rawKeyedStateInputs, rawOperatorStateInputs,
			new SimpleRandomService(), new SimpleTimeService(), new SimpleSerializableServiceFactory());

	}

	public StateInitializationContextImpl(boolean restored,
										  boolean isStandby,
										  OperatorStateStore operatorStateStore,
										  KeyedStateStore keyedStateStore,
										  Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs,
										  Iterable<StatePartitionStreamProvider> rawOperatorStateInputs,
										  RandomService randomService,
										  TimeService timestampService,
										  SerializableServiceFactory serializableServiceFactory) {
		this.restored = restored;
		this.isStandby = isStandby;
		this.operatorStateStore = operatorStateStore;
		this.keyedStateStore = keyedStateStore;
		this.rawOperatorStateInputs = rawOperatorStateInputs;
		this.rawKeyedStateInputs = rawKeyedStateInputs;
		this.randomService = randomService;
		this.timestampService = timestampService;
		this.serializableServiceFactory = serializableServiceFactory;
	}

	@Override
	public boolean isRestored() {
		return restored;
	}

	@Override
	public boolean isStandby() {
		return isStandby;
	}

	@Override
	public Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs() {
		return rawOperatorStateInputs;
	}

	@Override
	public Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs() {
		if (null == keyedStateStore) {
			throw new IllegalStateException("Attempt to access keyed state from non-keyed operator.");
		}

		return rawKeyedStateInputs;
	}

	@Override
	public OperatorStateStore getOperatorStateStore() {
		return operatorStateStore;
	}

	@Override
	public KeyedStateStore getKeyedStateStore() {
		return keyedStateStore;
	}

	@Override
	public RandomService getRandomService() {
		return randomService;
	}

	@Override
	public TimeService getTimeService() {
		return timestampService;
	}

	@Override
	public SerializableServiceFactory getSerializableServiceFactory() {
		return serializableServiceFactory;
	}
}
