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

import org.apache.flink.runtime.causal.determinant.*;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Timer;

//We use this cache to avoid object creation during replay
//We require two determinants of each type because while one is being processed, the next may already be
// a determinant of the same type
public class DeterminantPool {


	private static final int NUM_BASE_DETERMINANTS = 2;
	Queue<Determinant>[] determinantCache;

	public DeterminantPool(){
		determinantCache = new Queue[8];

		determinantCache[OrderDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[OrderDeterminant.getTypeTag()].add(new OrderDeterminant());

		determinantCache[TimestampDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[TimestampDeterminant.getTypeTag()].add(new TimestampDeterminant());

		determinantCache[RNGDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[RNGDeterminant.getTypeTag()].add(new RNGDeterminant());

		determinantCache[SerializableDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[SerializableDeterminant.getTypeTag()].add(new SerializableDeterminant());

		determinantCache[TimerTriggerDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[TimerTriggerDeterminant.getTypeTag()].add(new TimerTriggerDeterminant());

		determinantCache[SourceCheckpointDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[SourceCheckpointDeterminant.getTypeTag()].add(new SourceCheckpointDeterminant());

		determinantCache[IgnoreCheckpointDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[IgnoreCheckpointDeterminant.getTypeTag()].add(new IgnoreCheckpointDeterminant());

		determinantCache[BufferBuiltDeterminant.getTypeTag()] = new ArrayDeque<>();
		for(int i = 0; i < NUM_BASE_DETERMINANTS; i++)
			determinantCache[BufferBuiltDeterminant.getTypeTag()].add(new BufferBuiltDeterminant());
	}



	public void recycle(Determinant determinant) {
		if (determinant != null)
			determinantCache[determinant.getTag()].add(determinant);
	}

	public OrderDeterminant getOrderDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.ORDER_DETERMINANT_TAG];
		if(q.isEmpty())
			q.add(new OrderDeterminant());
		return (OrderDeterminant) q.poll();
	}

	public TimestampDeterminant getTimestampDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.TIMESTAMP_DETERMINANT_TAG];
		if(q.isEmpty())
			q.add(new TimestampDeterminant());
		return (TimestampDeterminant) q.poll();
	}

	public RNGDeterminant getRNGDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.RNG_DETERMINANT_TAG];
		if(q.isEmpty())
			q.add(new RNGDeterminant());
		return (RNGDeterminant) q.poll();
	}

	public TimerTriggerDeterminant getTimerTriggerDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.TIMER_TRIGGER_DETERMINANT];
		if(q.isEmpty())
			q.add(new TimerTriggerDeterminant());
		return (TimerTriggerDeterminant) q.poll();
	}

	public SourceCheckpointDeterminant getSourceCheckpointDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.SOURCE_CHECKPOINT_DETERMINANT];
		if(q.isEmpty())
			q.add(new SourceCheckpointDeterminant());
		return (SourceCheckpointDeterminant) q.poll();
	}

	public IgnoreCheckpointDeterminant getIgnoreCheckpointDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.IGNORE_CHECKPOINT_DETERMINANT];
		if(q.isEmpty())
			q.add(new IgnoreCheckpointDeterminant());
		return (IgnoreCheckpointDeterminant) q.poll();
	}

	public BufferBuiltDeterminant getBufferBuiltDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.BUFFER_BUILT_TAG];
		if(q.isEmpty())
			q.add(new BufferBuiltDeterminant());
		return (BufferBuiltDeterminant) q.poll();
	}

	public SerializableDeterminant getSerializableDeterminant() {
		Queue<Determinant> q = determinantCache[Determinant.SERIALIZABLE_DETERMINANT_TAG];
		if(q.isEmpty())
			q.add(new SerializableDeterminant());
		return (SerializableDeterminant) q.poll();
	}
}
