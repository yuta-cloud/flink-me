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

//We use this cache to avoid object creation during replay
//We require two determinants of each type because while one is being processed, the next may already be
// a determinant of the same type
public class DeterminantBufferPool {


	Queue<Determinant>[] determinantCache;

	public DeterminantBufferPool(){
		determinantCache = new Queue[7];

		determinantCache[OrderDeterminant.getTypeTag()] = new ArrayDeque<>();
		determinantCache[OrderDeterminant.getTypeTag()].add(new OrderDeterminant());
		determinantCache[OrderDeterminant.getTypeTag()].add(new OrderDeterminant());

		determinantCache[TimestampDeterminant.getTypeTag()] = new ArrayDeque<>();
		determinantCache[TimestampDeterminant.getTypeTag()].add(new TimestampDeterminant());
		determinantCache[TimestampDeterminant.getTypeTag()].add(new TimestampDeterminant());

		determinantCache[RNGDeterminant.getTypeTag()] = new ArrayDeque<>();
		determinantCache[RNGDeterminant.getTypeTag()].add(new RNGDeterminant());
		determinantCache[RNGDeterminant.getTypeTag()].add(new RNGDeterminant());

		determinantCache[TimerTriggerDeterminant.getTypeTag()] = new ArrayDeque<>();
		determinantCache[TimerTriggerDeterminant.getTypeTag()].add(new TimerTriggerDeterminant());
		determinantCache[TimerTriggerDeterminant.getTypeTag()].add(new TimerTriggerDeterminant());

		determinantCache[SourceCheckpointDeterminant.getTypeTag()] = new ArrayDeque<>();
		determinantCache[SourceCheckpointDeterminant.getTypeTag()].add(new SourceCheckpointDeterminant());
		determinantCache[SourceCheckpointDeterminant.getTypeTag()].add(new SourceCheckpointDeterminant());

		determinantCache[IgnoreCheckpointDeterminant.getTypeTag()] = new ArrayDeque<>();
		determinantCache[IgnoreCheckpointDeterminant.getTypeTag()].add(new IgnoreCheckpointDeterminant());
		determinantCache[IgnoreCheckpointDeterminant.getTypeTag()].add(new IgnoreCheckpointDeterminant());

		determinantCache[BufferBuiltDeterminant.getTypeTag()] = new ArrayDeque<>();
		determinantCache[BufferBuiltDeterminant.getTypeTag()].add(new BufferBuiltDeterminant());
		determinantCache[BufferBuiltDeterminant.getTypeTag()].add(new BufferBuiltDeterminant());
	}



	public void recycle(Determinant determinant) {
		if (determinant != null)
			determinantCache[determinant.getTag()].add(determinant);
	}

	public OrderDeterminant getOrderDeterminant() {
		return (OrderDeterminant) determinantCache[Determinant.ORDER_DETERMINANT_TAG].poll();
	}

	public TimestampDeterminant getTimestampDeterminant() {
		return (TimestampDeterminant) determinantCache[Determinant.TIMESTAMP_DETERMINANT_TAG].poll();
	}

	public RNGDeterminant getRNGDeterminant() {
		return (RNGDeterminant) determinantCache[Determinant.RNG_DETERMINANT_TAG].poll();
	}

	public TimerTriggerDeterminant getTimerTriggerDeterminant() {
		return (TimerTriggerDeterminant) determinantCache[Determinant.TIMER_TRIGGER_DETERMINANT].poll();
	}

	public SourceCheckpointDeterminant getSourceCheckpointDeterminant() {
		return (SourceCheckpointDeterminant) determinantCache[Determinant.SOURCE_CHECKPOINT_DETERMINANT].poll();
	}

	public IgnoreCheckpointDeterminant getIgnoreCheckpointDeterminant() {
		return (IgnoreCheckpointDeterminant) determinantCache[Determinant.IGNORE_CHECKPOINT_DETERMINANT].poll();
	}

	public BufferBuiltDeterminant getBufferBuiltDeterminant() {
		return (BufferBuiltDeterminant) determinantCache[Determinant.BUFFER_BUILT_TAG].poll();
	}
}
