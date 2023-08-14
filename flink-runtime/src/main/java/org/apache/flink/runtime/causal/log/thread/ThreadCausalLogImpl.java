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

package org.apache.flink.runtime.causal.log.thread;

import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.determinant.DeterminantEncoder;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThreadCausalLogImpl implements ThreadCausalLog {
	private static final Logger LOG = LoggerFactory.getLogger(ThreadCausalLogImpl.class);

	// We use this buffer pool to fetch memory segments to fill the composite bellow
	private final BufferPool bufferPool;

	// This composite is used to create an indefinitely growing buffer of determinants
	private final CompositeByteBuf buf;

	// The encoding strategy for encoding local determinants
	private final DeterminantEncoder determinantEncoder;

	//Locks used to protect epochStartOffsets
	private final Lock epochReadLock;
	private final Lock epochWriteLock;

	//Tracks the start of epochs in the determinant log. Reads must be protected with the epochReadLock because
	// asynchronous checkpoint completions (which acquire the epochWriteLock) may change the physical offsets of
	// epochs.
	private final ConcurrentMap<Long, EpochStartOffset> epochStartOffsets;

	// Tracks the consumer offsets in this causal log
	private final ConcurrentMap<InputChannelID, ConsumerOffset> channelOffsetMap;

	// Tracks up to where writes are visible, so that some thread can read up to this value, while another writes
	// ahead of this value
	private final AtomicInteger visibleWriterIndex;
	private final CausalLogID causalLogID;

	private final int determinantSharingDepth;

	private final int bufferComponentSize;
	/**
	 * This constructor is used for upstream logs as they do not require a determinant encoder
	 */
	public ThreadCausalLogImpl(BufferPool determinantBufferPool, CausalLogID causalLogID,
							   int determinantSharingDepth) {
		this(determinantBufferPool, causalLogID, determinantSharingDepth, null);
	}

	/**
	 * This constructor is used for local logs.
	 */
	public ThreadCausalLogImpl(BufferPool determinantBufferPool, CausalLogID causalLogID, int determinantSharingDepth,
							   DeterminantEncoder determinantEncoder) {
		this.bufferPool = determinantBufferPool;
		this.causalLogID = causalLogID;
		this.determinantEncoder = determinantEncoder;
		this.determinantSharingDepth = determinantSharingDepth;

		buf = ByteBufAllocator.DEFAULT.compositeDirectBuffer(Integer.MAX_VALUE);
		addComponent();

		epochStartOffsets = new ConcurrentHashMap<>();
		channelOffsetMap = new ConcurrentHashMap<>();
		visibleWriterIndex = new AtomicInteger(0);

		ReadWriteLock epochLock = new ReentrantReadWriteLock();
		epochReadLock = epochLock.readLock();
		epochWriteLock = epochLock.writeLock();
		this.bufferComponentSize = bufferPool.getMemorySegmentSize();
	}


	//========================= ONLY FOR UPSTREAM LOGS ==================================================
	@Override
	public void processUpstreamDelta(ByteBuf delta, int offsetFromEpoch, long epochID) {
		int determinantSize = delta.readableBytes();
		if (LOG.isDebugEnabled())
			LOG.debug("processUpstreamDelta: offsetFromEpoch: {}, epochID: {}, determinantSize: {}", offsetFromEpoch,
				epochID, determinantSize);
		epochReadLock.lock();
		try {
			if (determinantSize > 0) {
				synchronized (buf) {
					int writeIndex = visibleWriterIndex.get();
					EpochStartOffset epochStartOffset = epochStartOffsets.computeIfAbsent(epochID,
						k -> new EpochStartOffset(k, writeIndex));

					int currentLogicalOffsetFromEpoch = writeIndex - epochStartOffset.getOffset();

					int numNewDeterminants = (offsetFromEpoch + determinantSize) - currentLogicalOffsetFromEpoch;

					if (numNewDeterminants > 0) {

						while (notEnoughSpaceFor(numNewDeterminants))
							addComponent();
						if (LOG.isDebugEnabled())
							LOG.debug("processUpstreamDelta: writeIndex: {}, epochStartOffset: {}," +
									" currentLogicalOffsetFromEpoch: {}, numNewDeterminants: {}",
								writeIndex, epochStartOffset.getOffset(), currentLogicalOffsetFromEpoch,
								numNewDeterminants);
						delta.readerIndex(determinantSize - numNewDeterminants);
						//add the new determinants
						buf.writeBytes(delta, numNewDeterminants);
						visibleWriterIndex.addAndGet(numNewDeterminants);
					}
				}
			}

		} finally {
			epochReadLock.unlock();
		}
	}

	//========================= ONLY FOR LOCAL LOGS =========================================================
	@Override
	public void appendDeterminant(Determinant determinant, long epochID) {
		if (determinantSharingDepth == 0)
			return;
		int determinantEncodedSize = determinant.getEncodedSizeInBytes();
		if (LOG.isDebugEnabled())
			LOG.debug("appendDeterminant: Determinant: {}, epochID: {}, encodedSize: {}", determinant, epochID,
				determinantEncodedSize);
		epochReadLock.lock();
		try {
			synchronized (buf) {
				epochStartOffsets.computeIfAbsent(epochID, k -> new EpochStartOffset(k, visibleWriterIndex.get()));
				while (notEnoughSpaceFor(determinantEncodedSize))
					addComponent();
				determinantEncoder.encodeTo(determinant, buf, causalLogID.getVertexID());
				visibleWriterIndex.addAndGet(determinantEncodedSize);
			}
		} finally {
			epochReadLock.unlock();
		}
	}

	@Override
	public int logLength() {
		int result;
		epochReadLock.lock();
		try {
			Optional<Long> maybeFirstKey = epochStartOffsets.keySet().stream().min(Long::compareTo);
			result = maybeFirstKey
				.map(firstKey -> visibleWriterIndex.get() - epochStartOffsets.get(firstKey).getOffset())
				.orElseGet(visibleWriterIndex::get);
		} finally {
			epochReadLock.unlock();
		}
		return result;
	}

	//========================= FOR ALL LOGS =========================================================
	@Override
	public boolean hasDeltaForConsumer(InputChannelID outputChannelID, long epochID) {
		if (determinantSharingDepth == 0)
			return false;
		epochReadLock.lock();
		try {
			EpochStartOffset epochStartOffset = epochStartOffsets.get(epochID);
			if (epochStartOffset == null) { //If the epoch does not exist, there is certainly no delta
				if (LOG.isDebugEnabled())
					LOG.debug("hasDeltaForConsumer: outputChannel: {}, epochID: {}, returns early because " +
						"epochStartOffset " +
						"does not exist", outputChannelID, epochID);
				return false;
			}

			ConsumerOffset consumerOffset = channelOffsetMap.computeIfAbsent(outputChannelID,
				k -> new ConsumerOffset(epochStartOffset));

			long currentConsumerEpochID = consumerOffset.getEpochStart().getId();
			if (currentConsumerEpochID != epochID) {
				if (currentConsumerEpochID > epochID)
					throw new RuntimeException("Consumer went backwards, current epoch " + currentConsumerEpochID +
						" " +
						"requested " + epochID);
				//else if (currentConsumerEpochID < epochID -1)
				//	throw new RuntimeException("Consumer skipped an epoch, current epoch " + currentConsumerEpochID +
				//		" " +
				//		"requested " + epochID);

				consumerOffset.epochStart = epochStartOffset;
				consumerOffset.offset = 0;
			}

			int physicalConsumerOffset = consumerOffset.epochStart.offset + consumerOffset.offset;
			int numBytesToSend = computeNumberOfBytesToSend(epochID, physicalConsumerOffset);

			if (LOG.isDebugEnabled())
				LOG.debug("hasDeltaForConsumer: outputChannel: {}, epochID: {}, physicalConsumerOffset: {}, " +
					"numBytesToSend:" +
					" {}", outputChannelID, epochID, physicalConsumerOffset, numBytesToSend);
			//If the epoch exists, then there is a delta if there are any bytes to send
			return numBytesToSend != 0;
		} finally {
			epochReadLock.unlock();
		}
	}

	@Override
	public int getOffsetFromEpochForConsumer(InputChannelID outputChannelID, long epochID) {
		//Certainly exists because protected by hasDeltaForConsumer
		return channelOffsetMap.get(outputChannelID).getOffset();
	}

	@Override
	public ByteBuf getDeltaForConsumer(InputChannelID outputChannelID, long epochID) {
		//If a request is coming for the next determinants, then the epoch MUST already exist.
		epochReadLock.lock();
		try {
			//Exists because protected by hasDeltaForConsumer
			ConsumerOffset consumerOffset = channelOffsetMap.get(outputChannelID);

			int physicalConsumerOffset = consumerOffset.epochStart.offset + consumerOffset.offset;

			int numBytesToSend = computeNumberOfBytesToSend(epochID, physicalConsumerOffset);
			ByteBuf update;

			if (LOG.isDebugEnabled())
				LOG.debug("getDeltaForConsumer: epoch {}, physConsOffset {}. numBytesToSend {}, consumerOffset: {}",
					epochID, physicalConsumerOffset, numBytesToSend, consumerOffset.getOffset());
			if (numBytesToSend == 0)
				update = Unpooled.EMPTY_BUFFER;
			else
				synchronized (buf) {
					update = makeDeltaUnsafe(physicalConsumerOffset, numBytesToSend);
				}

			ByteBuf toReturn = update;
			consumerOffset.setOffset(consumerOffset.getOffset() + numBytesToSend);
			return toReturn;
		} finally {
			epochReadLock.unlock();
		}
	}

	@Override
	public CausalLogID getCausalLogID() {
		return causalLogID;
	}

	@Override
	public ByteBuf getDeterminants(long startEpochID) {
		if (determinantSharingDepth == 0)
			return Unpooled.EMPTY_BUFFER;

		ByteBuf result;
		int startIndex = 0;

		epochReadLock.lock();
		try {
			EpochStartOffset offset = epochStartOffsets.get(startEpochID);
			if (offset != null)
				startIndex = offset.getOffset();
			else {
				long earliestEpochID = epochStartOffsets.keySet().stream().min(Long::compareTo).orElse(-1L);
				if (earliestEpochID != -1L)
					startIndex = epochStartOffsets.get(earliestEpochID).getOffset();
			}
			int writerPos = visibleWriterIndex.get();
			int numBytesToSend = writerPos - startIndex;

			synchronized (buf) {
				result = makeDeltaUnsafe(startIndex, numBytesToSend);
			}

		} finally {
			epochReadLock.unlock();
		}
		return result;
	}


	@Override
	public void close() {
		while (!fullyConsumed()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		epochWriteLock.lock();
		buf.release();
		epochWriteLock.unlock();
	}

	@Override
	public void unregisterConsumer(InputChannelID toCancel) {
		epochReadLock.lock();
		channelOffsetMap.remove(toCancel);
		epochReadLock.unlock();

	}

	private boolean fullyConsumed() {
		boolean fullyConsumed = true;
		epochWriteLock.lock();
		for (ConsumerOffset co : channelOffsetMap.values())
			if (co.getEpochStart().getOffset() + co.getOffset() < visibleWriterIndex.get()) {
				fullyConsumed = false;
				break;
			}
		epochWriteLock.unlock();
		return fullyConsumed;
	}


	private boolean notEnoughSpaceFor(int length) {
		return buf.writableBytes() < length;
	}

	/*
	 * Build a composite byte buffer using the log internal components. Doing so guarantees that even if a
	 * checkpoint happens, sliced internal components data will not be moved.
	 * Thus, as long as this is called within the reader lock, we are safe to not copy the data.
	 * This is because of the invariant that a checkpoint cannot complete if there are still unsent determinants
	 * of an epoch.
	 *
	 * NOTE: Uses must be wrapped by reader lock
	 */
	private ByteBuf makeDeltaUnsafe(int srcOffset, int numBytesToSend) {

		CompositeByteBuf result = ByteBufAllocator.DEFAULT.compositeDirectBuffer(Integer.MAX_VALUE);

		int currIndex = srcOffset;
		int numBytesLeft = numBytesToSend;
		while (numBytesLeft != 0) {
			int bufferIndex = currIndex / bufferComponentSize;
			int indexInBuffer = currIndex % bufferComponentSize;
			ByteBuf component = buf.internalComponent(bufferIndex);
			int numBytesFromBuf = Math.min(numBytesLeft, bufferComponentSize - indexInBuffer);
			if (numBytesFromBuf > 0)
				result.addComponent(true, component.retainedSlice(indexInBuffer, numBytesFromBuf));

			numBytesLeft -= numBytesFromBuf;
			currIndex += numBytesFromBuf;
		}
		return result;
	}

	private int computeNumberOfBytesToSend(long epochID, int physicalConsumerOffset) {
		int currentWriteIndex = visibleWriterIndex.get();
		EpochStartOffset nextEpochStartOffset =
			epochStartOffsets.get(epochID + 1);
		int numBytesToSend;

		if (nextEpochStartOffset != null)
			numBytesToSend = nextEpochStartOffset.getOffset() - physicalConsumerOffset;
		else
			numBytesToSend = currentWriteIndex - physicalConsumerOffset;
		return numBytesToSend;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		if (LOG.isDebugEnabled())
			LOG.debug("Notify checkpoint complete for id {}", checkpointId);
		epochWriteLock.lock();
		try {
			int visibleWriter = visibleWriterIndex.get();
			EpochStartOffset followingEpoch =
				epochStartOffsets.computeIfAbsent(checkpointId,
					epochID -> new EpochStartOffset(epochID,
						visibleWriter));
			for (Long epochID : epochStartOffsets.keySet())
				if (epochID < checkpointId)
					epochStartOffsets.remove(epochID);

			int followingEpochOffset = followingEpoch.getOffset();
			if (LOG.isDebugEnabled())
				LOG.debug("chkComplete visWriterIndex {}, followingEpochOffset {}", visibleWriter,
					followingEpochOffset);
			buf.readerIndex(followingEpochOffset);
			buf.discardReadComponents();
			int move = followingEpochOffset - buf.readerIndex();

			if (LOG.isDebugEnabled())
				LOG.debug("Offsets moved by {} bytes.", move);
			for (Map.Entry<Long, EpochStartOffset> entry :
				epochStartOffsets.entrySet()) {
				EpochStartOffset eso = entry.getValue();
				int currentOffset = eso.getOffset();
				LOG.debug("Epoch {} currently at {} moved by {} and moved to {}", entry.getKey(), currentOffset, move,
					currentOffset - move);

				eso.setOffset(currentOffset - move);
			}
			visibleWriterIndex.set(visibleWriterIndex.get() - move);
		} finally {
			epochWriteLock.unlock();
		}
	}


	private void addComponent() {
		if (LOG.isDebugEnabled())
			LOG.debug("Adding component, composite size: {}", buf.capacity());
		Buffer buffer = null;

		try {
			buffer = bufferPool.requestBufferBlocking();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		ByteBuf byteBuf = buffer.asByteBuf();
		//The writer index movement tricks netty into adding to the composite capacity.
		byteBuf.writerIndex(byteBuf.capacity());
		buf.addComponent(byteBuf);
	}

	private static class EpochStartOffset {
		//The checkpoint id that initiates this epoch
		private long id;
		//The physical offset in the log of the first element after the checkpoint
		private int offset;


		public EpochStartOffset(long id, int offset) {
			this.id = id;
			this.offset = offset;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}


		@Override
		public String toString() {
			return "CheckpointOffset{" +
				"id=" + id +
				", offset=" + offset +
				'}';
		}
	}

	/**
	 * Marks the next element to be read by the downstream consumer
	 */
	private static class ConsumerOffset {
		// Refers to the epoch that the downstream is currently in
		private EpochStartOffset epochStart;

		// The logical offset from that epoch
		private int offset;

		public ConsumerOffset(EpochStartOffset epochStart) {
			this.epochStart = epochStart;
			this.offset = 0;
		}

		public EpochStartOffset getEpochStart() {
			return epochStart;
		}

		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}

		@Override
		public String toString() {
			return "DownstreamChannelOffset{" +
				"epochStart=" + epochStart +
				", offset=" + offset +
				'}';
		}
	}
}
