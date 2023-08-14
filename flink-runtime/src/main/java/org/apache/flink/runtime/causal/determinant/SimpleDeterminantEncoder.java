/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.causal.determinant;


import org.apache.flink.runtime.causal.recovery.DeterminantPool;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.*;

public class SimpleDeterminantEncoder implements DeterminantEncoder {
	private final BlockingQueue<ByteBuf> sendQueue; //Causal Log send queue

	//Constractor for MeTCPServer
	public SimpleDeterminantEncoder(){
		sendQueue = new LinkedBlockingQueue<>();
		Thread serverThread = new Thread(() -> {
            new MeTCPServer(sendQueue).run();
        });
        serverThread.start();
	}

	@Override
	public byte[] encode(Determinant determinant) {
		if (determinant.isOrderDeterminant())
			return encodeOrderDeterminant(determinant.asOrderDeterminant());
		if (determinant.isTimestampDeterminant())
			return encodeTimestampDeterminant(determinant.asTimestampDeterminant());
		if (determinant.isRNGDeterminant())
			return encodeRNGDeterminant(determinant.asRNGDeterminant());
		if (determinant.isSerializableDeterminant())
			return encodeSerializableDeterminant(determinant.asSerializableDeterminant());
		if (determinant.isBufferBuiltDeterminant())
			return encodeBufferBuiltDeterminant(determinant.asBufferBuiltDeterminant());
		if (determinant.isTimerTriggerDeterminant())
			return encodeTimerTriggerDeterminant(determinant.asTimerTriggerDeterminant());
		if (determinant.isSourceCheckpointDeterminant())
			return encodeSourceCheckpointDeterminant(determinant.asSourceCheckpointDeterminant());
		if (determinant.isIgnoreCheckpointDeterminant())
			return encodeIgnoreCheckpointDeterminant(determinant.asIgnoreCheckpointDeterminant());
		throw new UnknownDeterminantTypeException();
	}

	//Leader sends determinant to followers
	@Override
	public void encodeTo(Determinant determinant, ByteBuf targetBuf, short vertexID) {
		if (determinant.isOrderDeterminant())
			encodeOrderDeterminant(determinant.asOrderDeterminant(), targetBuf, vertexID);
		else if (determinant.isTimestampDeterminant())
			encodeTimestampDeterminant(determinant.asTimestampDeterminant(), targetBuf, vertexID);
		else if (determinant.isRNGDeterminant())
			encodeRNGDeterminant(determinant.asRNGDeterminant(), targetBuf, vertexID);
		else if (determinant.isSerializableDeterminant())
			encodeSerializableDeterminant(determinant.asSerializableDeterminant(), targetBuf, vertexID);
		else if (determinant.isBufferBuiltDeterminant())
			encodeBufferBuiltDeterminant(determinant.asBufferBuiltDeterminant(), targetBuf, vertexID);
		else if (determinant.isTimerTriggerDeterminant())
			encodeTimerTriggerDeterminant(determinant.asTimerTriggerDeterminant(), targetBuf, vertexID);
		else if (determinant.isSourceCheckpointDeterminant())
			encodeSourceCheckpointDeterminant(determinant.asSourceCheckpointDeterminant(), targetBuf, vertexID);
		else if (determinant.isIgnoreCheckpointDeterminant())
			encodeIgnoreCheckpointDeterminant(determinant.asIgnoreCheckpointDeterminant(), targetBuf, vertexID);
		else
			throw new UnknownDeterminantTypeException();
	}

	@Override
	public Determinant decodeNext(ByteBuf b) {
		if (b == null)
			return null;
		if (!b.isReadable())
			return null;
		byte tag = b.readByte();
		if (tag == Determinant.ORDER_DETERMINANT_TAG) return decodeOrderDeterminant(b);
		if (tag == Determinant.TIMESTAMP_DETERMINANT_TAG) return decodeTimestampDeterminant(b);
		if (tag == Determinant.RNG_DETERMINANT_TAG) return decodeRNGDeterminant(b);
		if (tag == Determinant.SERIALIZABLE_DETERMINANT_TAG) return decodeSerializableDeterminant(b);
		if (tag == Determinant.BUFFER_BUILT_TAG) return decodeBufferBuiltDeterminant(b);
		if (tag == Determinant.TIMER_TRIGGER_DETERMINANT) return decodeTimerTriggerDeterminant(b);
		if (tag == Determinant.SOURCE_CHECKPOINT_DETERMINANT) return decodeSourceCheckpointDeterminant(b);
		if (tag == Determinant.IGNORE_CHECKPOINT_DETERMINANT) return decodeIgnoreCheckpointDeterminant(b);
		throw new CorruptDeterminantArrayException(tag);
	}


	@Override
	public Determinant decodeNext(ByteBuf b, DeterminantPool determinantCache) {
		if (b == null)
			return null;
		if (!b.isReadable())
			return null;
		byte tag = b.readByte();
		if (tag == Determinant.ORDER_DETERMINANT_TAG) return decodeOrderDeterminant(b, determinantCache.getOrderDeterminant());
		if (tag == Determinant.TIMESTAMP_DETERMINANT_TAG) return decodeTimestampDeterminant(b, determinantCache.getTimestampDeterminant());
		if (tag == Determinant.RNG_DETERMINANT_TAG) return decodeRNGDeterminant(b, determinantCache.getRNGDeterminant());
		if (tag == Determinant.SERIALIZABLE_DETERMINANT_TAG) return decodeSerializableDeterminant(b, determinantCache.getSerializableDeterminant());
		if (tag == Determinant.BUFFER_BUILT_TAG) return decodeBufferBuiltDeterminant(b, determinantCache.getBufferBuiltDeterminant());
		if (tag == Determinant.TIMER_TRIGGER_DETERMINANT) return decodeTimerTriggerDeterminant(b, determinantCache.getTimerTriggerDeterminant());
		if (tag == Determinant.SOURCE_CHECKPOINT_DETERMINANT) return decodeSourceCheckpointDeterminant(b, determinantCache.getSourceCheckpointDeterminant());
		if (tag == Determinant.IGNORE_CHECKPOINT_DETERMINANT) return decodeIgnoreCheckpointDeterminant(b, determinantCache.getIgnoreCheckpointDeterminant());
		throw new CorruptDeterminantArrayException(tag);
	}


	@Override
	public Determinant decodeOrderDeterminant(ByteBuf b) {
		return decodeOrderDeterminant(b, new OrderDeterminant());
	}
	@Override
	public Determinant decodeOrderDeterminant(ByteBuf b, OrderDeterminant reuse) {
		return reuse.replace(b.readByte());
	}

	private void encodeOrderDeterminant(OrderDeterminant orderDeterminant, ByteBuf buf, short vertexID) {
		buf.writeByte(Determinant.ORDER_DETERMINANT_TAG);
		buf.writeByte(orderDeterminant.getChannel());
		//Send order determinant to followers
		ByteBuf buf_me = Unpooled.buffer(orderDeterminant.getEncodedSizeInBytes() + 2);
		buf_me.writeByte(vertexID);
		buf_me.writeByte(Determinant.ORDER_DETERMINANT_TAG);
		buf_me.writeByte(orderDeterminant.getChannel());
		sendQueue.add(buf_me);
	}

	private byte[] encodeOrderDeterminant(OrderDeterminant orderDeterminant) {
		byte[] bytes = new byte[orderDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeOrderDeterminant(orderDeterminant, buf, (short)0);
		return bytes;
	}

	@Override
	public Determinant decodeTimestampDeterminant(ByteBuf b) {
		return decodeTimestampDeterminant(b, new TimestampDeterminant());
	}

	@Override
	public Determinant decodeTimestampDeterminant(ByteBuf b, TimestampDeterminant reuse) {
		return reuse.replace(b.readLong());
	}
	private void encodeTimestampDeterminant(TimestampDeterminant timestampDeterminant, ByteBuf buf, short vertexID) {
		buf.writeByte(Determinant.TIMESTAMP_DETERMINANT_TAG);
		buf.writeLong(timestampDeterminant.getTimestamp());
		//Send timestamp determinant to followers
		ByteBuf buf_me = Unpooled.buffer(timestampDeterminant.getEncodedSizeInBytes());
		buf_me.writeByte(vertexID);
		buf_me.writeByte(Determinant.TIMESTAMP_DETERMINANT_TAG);
		buf_me.writeLong(timestampDeterminant.getTimestamp());
		sendQueue.add(buf_me);
	}

	private byte[] encodeTimestampDeterminant(TimestampDeterminant timestampDeterminant) {
		byte[] bytes = new byte[timestampDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeTimestampDeterminant(timestampDeterminant, buf, (short)0);
		return bytes;
	}

	@Override
	public Determinant decodeRNGDeterminant(ByteBuf b) {
		return decodeRNGDeterminant(b, new RNGDeterminant());
	}

	@Override
	public Determinant decodeRNGDeterminant(ByteBuf b, RNGDeterminant reuse) {
		return reuse.replace(b.readInt());
	}

	private void encodeRNGDeterminant(RNGDeterminant rngDeterminant, ByteBuf buf, short vertexID) {
		buf.writeByte(Determinant.RNG_DETERMINANT_TAG);
		buf.writeInt(rngDeterminant.getNumber());
		//Send random seed determinant to followers
		ByteBuf buf_me = Unpooled.buffer(rngDeterminant.getEncodedSizeInBytes());
		buf_me.writeByte(vertexID);
		buf_me.writeByte(Determinant.RNG_DETERMINANT_TAG);
		buf_me.writeInt(rngDeterminant.getNumber());
		sendQueue.add(buf_me);
	}

	private byte[] encodeRNGDeterminant(RNGDeterminant rngDeterminant) {
		byte[] bytes = new byte[rngDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeRNGDeterminant(rngDeterminant, buf, (short)0);
		return bytes;
	}

	@Override
	public Determinant decodeBufferBuiltDeterminant(ByteBuf b) {
		return decodeBufferBuiltDeterminant(b, new BufferBuiltDeterminant());
	}

	@Override
	public Determinant decodeBufferBuiltDeterminant(ByteBuf b, BufferBuiltDeterminant reuse) {
		return reuse.replace(b.readInt());
	}

	private void encodeBufferBuiltDeterminant(BufferBuiltDeterminant bufferBuiltDeterminant, ByteBuf buf, short vertexID) {
		buf.writeByte(Determinant.BUFFER_BUILT_TAG);
		buf.writeInt(bufferBuiltDeterminant.getNumberOfBytes());
		//Send buffer built determinant to followers
		ByteBuf buf_me = Unpooled.buffer(bufferBuiltDeterminant.getEncodedSizeInBytes());
		buf_me.writeByte(vertexID);
		buf_me.writeByte(Determinant.BUFFER_BUILT_TAG);
		buf_me.writeInt(bufferBuiltDeterminant.getNumberOfBytes());
		sendQueue.add(buf_me);
	}

	private byte[] encodeBufferBuiltDeterminant(BufferBuiltDeterminant bufferBuiltDeterminant) {
		byte[] bytes = new byte[bufferBuiltDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeBufferBuiltDeterminant(bufferBuiltDeterminant, buf, (short)0);
		return bytes;

	}

	private void encodeTimerTriggerDeterminant(TimerTriggerDeterminant determinant, ByteBuf buf, short vertexID) {
		ProcessingTimeCallbackID id = determinant.getProcessingTimeCallbackID();

		buf.writeByte(Determinant.TIMER_TRIGGER_DETERMINANT);
		buf.writeInt(determinant.getRecordCount());
		buf.writeLong(determinant.getTimestamp());
		buf.writeByte((byte) id.getType().ordinal());
		if (id.getType() == ProcessingTimeCallbackID.Type.INTERNAL) {
			buf.writeInt(id.getName().getBytes().length);
			buf.writeBytes(id.getName().getBytes());
		}

		//Send timer trigger determinant to followers
		ByteBuf buf_me = Unpooled.buffer(determinant.getEncodedSizeInBytes());
		buf_me.writeByte(vertexID);
		buf_me.writeByte(Determinant.TIMER_TRIGGER_DETERMINANT);
		buf_me.writeInt(determinant.getRecordCount());
		buf_me.writeLong(determinant.getTimestamp());
		buf_me.writeByte((byte) id.getType().ordinal());
		if (id.getType() == ProcessingTimeCallbackID.Type.INTERNAL) {
			buf_me.writeInt(id.getName().getBytes().length);
			buf_me.writeBytes(id.getName().getBytes());
		}
		sendQueue.add(buf_me);
	}

	private byte[] encodeTimerTriggerDeterminant(TimerTriggerDeterminant determinant) {
		byte[] bytes = new byte[determinant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeTimerTriggerDeterminant(determinant, buf, (short)0);
		return bytes;
	}

	@Override
	public Determinant decodeTimerTriggerDeterminant(ByteBuf b) {
		return decodeTimerTriggerDeterminant(b, new TimerTriggerDeterminant());
	}

	@Override
	public Determinant decodeTimerTriggerDeterminant(ByteBuf b, TimerTriggerDeterminant reuse){
		int recordCount = b.readInt();
		long timestamp = b.readLong();
		ProcessingTimeCallbackID.Type type = ProcessingTimeCallbackID.Type.values()[b.readByte()];
		ProcessingTimeCallbackID id;
		if (type == ProcessingTimeCallbackID.Type.INTERNAL) {
			int numBytesOfName = b.readInt();
			byte[] nameBytes = new byte[numBytesOfName];
			b.readBytes(nameBytes);
			id = new ProcessingTimeCallbackID(new String(nameBytes));
		} else {
			id = new ProcessingTimeCallbackID(type);
		}
		return reuse.replace(recordCount, id, timestamp);
	}

	private void encodeSourceCheckpointDeterminant(SourceCheckpointDeterminant det, ByteBuf buf, short vertexID) {
		byte[] ref = det.getStorageReference();

		buf.writeByte(Determinant.SOURCE_CHECKPOINT_DETERMINANT);
		buf.writeInt(det.getRecordCount());
		buf.writeLong(det.getCheckpointID());
		buf.writeLong(det.getCheckpointTimestamp());
		buf.writeByte((byte) det.getType().ordinal());
		buf.writeByte((byte) (ref == null ? 0 : 1));
		if (ref != null) {
			buf.writeInt(ref.length);
			buf.writeBytes(ref);
		}

		//Send encode source checkpoint determinant to followers
		ByteBuf buf_me = Unpooled.buffer(det.getEncodedSizeInBytes());
		buf_me.writeByte(vertexID);
		buf_me.writeByte(Determinant.SOURCE_CHECKPOINT_DETERMINANT);
		buf_me.writeInt(det.getRecordCount());
		buf_me.writeLong(det.getCheckpointID());
		buf_me.writeLong(det.getCheckpointTimestamp());
		buf_me.writeByte((byte) det.getType().ordinal());
		buf_me.writeByte((byte) (ref == null ? 0 : 1));
		if (ref != null) {
			buf_me.writeInt(ref.length);
			buf_me.writeBytes(ref);
		}
		sendQueue.add(buf_me);
	}

	private byte[] encodeSourceCheckpointDeterminant(SourceCheckpointDeterminant det) {
		// tag (1), rec count (4), checkpoint (8), ts (8), type ordinal (1), has ref? (1), ref length (4), ref (X)
		byte[] bytes = new byte[det.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeSourceCheckpointDeterminant(det, buf, (short)0);
		return bytes;
	}

	@Override
	public Determinant decodeSourceCheckpointDeterminant(ByteBuf b) {
		return decodeSourceCheckpointDeterminant(b, new SourceCheckpointDeterminant());
	}

	@Override
	public Determinant decodeSourceCheckpointDeterminant(ByteBuf b, SourceCheckpointDeterminant reuse) {
		int recCount = b.readInt();
		long checkpoint = b.readLong();
		long ts = b.readLong();
		byte typeOrd = b.readByte();
		boolean hasRef = b.readBoolean();
		byte[] ref = null;
		if (hasRef) {
			int length = b.readInt();
			ref = new byte[length];
			b.readBytes(ref);
		}
		return reuse.replace(recCount, checkpoint, ts, CheckpointType.values()[typeOrd], ref);

	}

	private void encodeIgnoreCheckpointDeterminant(IgnoreCheckpointDeterminant det, ByteBuf buf, short vertexID) {
		buf.writeByte(Determinant.IGNORE_CHECKPOINT_DETERMINANT);
		buf.writeInt(det.getRecordCount());
		buf.writeLong(det.getCheckpointID());

		//send encode ignore checkpoint determinant to followers
		ByteBuf buf_me = Unpooled.buffer(det.getEncodedSizeInBytes());
		buf_me.writeByte(vertexID);
		buf_me.writeByte(Determinant.IGNORE_CHECKPOINT_DETERMINANT);
		buf_me.writeInt(det.getRecordCount());
		buf_me.writeLong(det.getCheckpointID());
		sendQueue.add(buf_me);
	}

	private byte[] encodeIgnoreCheckpointDeterminant(IgnoreCheckpointDeterminant det) {
		// tag (1), rec count (4), checkpoint (8)
		byte[] bytes = new byte[det.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeIgnoreCheckpointDeterminant(det, buf, (short)0);
		return bytes;
	}

	@Override
	public Determinant decodeIgnoreCheckpointDeterminant(ByteBuf b) {
		return decodeIgnoreCheckpointDeterminant(b, new IgnoreCheckpointDeterminant());
	}

	@Override
	public Determinant decodeIgnoreCheckpointDeterminant(ByteBuf b, IgnoreCheckpointDeterminant reuse) {
		int recCount = b.readInt();
		long checkpoint = b.readLong();
		return reuse.replace(recCount, checkpoint);

	}

	private void encodeSerializableDeterminant(SerializableDeterminant serializableDeterminant, ByteBuf buf, short vertexID) {
		try {
			buf.writeByte(Determinant.SERIALIZABLE_DETERMINANT_TAG);
			ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
			ObjectOutputStream oos = new ObjectOutputStream(bbos);
			oos.writeObject(serializableDeterminant.getDeterminant());

			//Send serializable determinant to followers
			ByteBuf buf_me = Unpooled.buffer(serializableDeterminant.getEncodedSizeInBytes());
			buf_me.writeByte(vertexID);
			buf_me.writeByte(Determinant.SERIALIZABLE_DETERMINANT_TAG);
			ByteBufOutputStream bbos_me = new ByteBufOutputStream(buf_me);
			ObjectOutputStream oos_me = new ObjectOutputStream(bbos_me);
			oos_me.writeObject(serializableDeterminant.getDeterminant());
			sendQueue.add(buf_me);
		}catch (Exception e){e.printStackTrace();}
	}
	private byte[] encodeSerializableDeterminant(SerializableDeterminant serializableDeterminant) {
		byte[] bytes = new byte[serializableDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeSerializableDeterminant(serializableDeterminant, buf, (short)0);
		return bytes;
	}
	private Determinant decodeSerializableDeterminant(ByteBuf b) {
		return decodeSerializableDeterminant(b, new SerializableDeterminant());
	}
	private Determinant decodeSerializableDeterminant(ByteBuf b, SerializableDeterminant reuse) {

		try {
			ByteBufInputStream bbis = new ByteBufInputStream(b);
			ObjectInputStream ois = new ObjectInputStream(bbis);
			reuse.replace(ois.readObject());
		}catch (Exception e) {e.printStackTrace();}
		return reuse;
	}
}
