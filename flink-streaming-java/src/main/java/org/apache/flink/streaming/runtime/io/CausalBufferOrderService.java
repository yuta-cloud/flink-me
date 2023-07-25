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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.causal.determinant.OrderDeterminant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.causal.services.AbstractCausalService;
import org.apache.flink.runtime.causal.services.BufferOrderService;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.flink.runtime.causal.recovery.MeConfig;

/**
 * A service that delivers deterministically ordered buffers from the {@link CheckpointBarrierHandler}.
 * <p>
 * This service is slightly more complex than other services as it returns the actual buffers and not just the
 * channel order determinant value.
 */
public class CausalBufferOrderService extends AbstractCausalService implements BufferOrderService {

	private static final Logger LOG = LoggerFactory.getLogger(CausalBufferOrderService.class);

	// The place from where we pull buffers in order to order them.
	private final CheckpointBarrierHandler bufferSource;

	// We use this to buffer buffers from the incorrect channels in order to deliver them in correct order
	private final Queue<BufferOrEvent>[] bufferedBuffersPerChannel;

	// The determinant object we reuse to avoid object creation and thus large GC
	private final OrderDeterminant reuseOrderDeterminant;

	// The number of input channels this task has. Important for buffering and exception cases such as = 1
	private final int numInputChannels;

	// If there are any buffered buffers, we need to check the queues
	private int numBufferedBuffers;

	private final MeConfig config = new MeConfig();

	public CausalBufferOrderService(JobCausalLog jobCausalLog, IRecoveryManager recoveryManager,
									CheckpointBarrierHandler bufferSource,
									int numInputChannels) {
		super(jobCausalLog, recoveryManager);
		this.bufferSource = bufferSource;
		this.bufferedBuffersPerChannel = new ArrayDeque[numInputChannels];
		for (int i = 0; i < numInputChannels; i++)
			bufferedBuffersPerChannel[i] = new ArrayDeque<>(100);
		this.numInputChannels = numInputChannels;
		this.numBufferedBuffers = 0;
		this.reuseOrderDeterminant = new OrderDeterminant();
	}

	@Override
	public BufferOrEvent getNextBuffer() throws Exception {


		if(LOG.isDebugEnabled())
			LOG.debug("Request next buffer");
		//Simple case, when there is only one channel we do not need to store order determinants, nor
		// do any special replay logic, because everything is deterministic.
		if (numInputChannels == 1) {
			return getNewBuffer();
		}

		BufferOrEvent toReturn;
		//if (isRecovering()) {
		if(!config.isLeader()){
			if(LOG.isDebugEnabled())
				LOG.debug("Get replayed buffer");
			toReturn = getNextNonBlockedReplayed();
		} else {
			if(LOG.isDebugEnabled())
				LOG.debug("Get new buffer");
			if(numBufferedBuffers != 0) {
				if(LOG.isDebugEnabled())
					LOG.debug("Get buffered buffer");
				toReturn = pickBufferedUnprocessedBuffer();
			} else {
				if(LOG.isDebugEnabled())
					LOG.debug("Get actual new buffer");
				toReturn = getNewBuffer();
			}
		}

		if (toReturn != null)
			threadCausalLog.appendDeterminant(reuseOrderDeterminant.replace((byte) toReturn.getChannelIndex()),
				epochTracker.getCurrentEpoch());

		return toReturn;
	}

	private BufferOrEvent getNextNonBlockedReplayed() throws Exception {
		BufferOrEvent toReturn;
		byte channel = recoveryManager.getLogReplayer().replayNextChannel();
		LOG.debug("Determinant says next channel is {}!", channel);
		if (bufferedBuffersPerChannel[channel].isEmpty()) {
			toReturn = processUntilFindBufferForChannel(channel);
		} else {
			toReturn = bufferedBuffersPerChannel[channel].remove();
			numBufferedBuffers--;
		}
		return toReturn;
	}

	private BufferOrEvent pickBufferedUnprocessedBuffer() {
		//todo improve runtime complexity
		for (Queue<BufferOrEvent> queue : bufferedBuffersPerChannel) {
			if (!queue.isEmpty()) {
				numBufferedBuffers--;
				return queue.remove();
			}
		}
		return null;//unrecheable
	}

	private BufferOrEvent processUntilFindBufferForChannel(byte channel) throws Exception {
		LOG.debug("Found no buffered buffers for channel {}. Processing buffers until I find one", channel);
		while (true) {
			BufferOrEvent newBufferOrEvent = getNewBuffer();
			if(newBufferOrEvent == null)
				continue;
			//If this was a BoE for the channel we were looking for, return with it
			if (newBufferOrEvent.getChannelIndex() == channel) {
				LOG.debug("It is from the expected channel, returning");
				return newBufferOrEvent;
			}

			LOG.debug("It is not from the expected channel,  buffering and continuing");
			//Otherwise, append it to the correct queue and try again
			bufferedBuffersPerChannel[newBufferOrEvent.getChannelIndex()].add(newBufferOrEvent);
			numBufferedBuffers++;
		}
	}

	private BufferOrEvent getNewBuffer() throws Exception {
		BufferOrEvent newBufferOrEvent;
		while (true) {
			newBufferOrEvent = bufferSource.getNextNonBlocked();
			if(newBufferOrEvent == null)
				return null;
			LOG.debug("Got a new buffer from channel {}", newBufferOrEvent.getChannelIndex());
			if (newBufferOrEvent.isEvent() && newBufferOrEvent.getEvent().getClass() == DeterminantRequestEvent.class) {
				LOG.debug("Buffer is DeterminantRequest, sending notification");
				recoveryManager.notifyDeterminantRequestEvent((DeterminantRequestEvent) newBufferOrEvent.getEvent(),
					newBufferOrEvent.getChannelIndex());
				continue;
			}
			break;
		}
		return newBufferOrEvent;
	}
}
