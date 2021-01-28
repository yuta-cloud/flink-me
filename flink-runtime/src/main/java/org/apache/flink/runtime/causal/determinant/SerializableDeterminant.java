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

package org.apache.flink.runtime.causal.determinant;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

/**
 * A serializable determinant is a determinant which contains a Serializable Object.
 * The advantage is that users do not have to concern themselves with serialization.
 * The disadvantage is that we must serialize the object twice, once to find the size of the serialized object, and again to serialize it into the log.
 * todo look into implementing an alternative where users can specify the serialization routines and encodedSize directly
 */
public class SerializableDeterminant extends Determinant {

	private Object determinant;

	public SerializableDeterminant(){

	}

	public SerializableDeterminant(Object determinant){
		this.determinant = determinant;
	}

	public SerializableDeterminant replace(Object determinant){
		this.determinant = determinant;
		return this;
	}

	public Object getDeterminant(){
		return determinant;
	}


	@Override
	public String toString() {
		return "SerializableDeterminant{" +
			"determinant=" + determinant +
			'}';
	}

	@Override
	public int getEncodedSizeInBytes() {
		return super.getEncodedSizeInBytes() + getEncodedSizeInBytesFromSerialization();
	}

	private int getEncodedSizeInBytesFromSerialization() {
		try {
			ByteArrayOutputStream boos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(boos);
			oos.writeObject(determinant);
			return boos.size();
		}catch (Exception e){e.printStackTrace();}
		return -1;
	}

	public static byte getTypeTag() {
		return SERIALIZABLE_DETERMINANT_TAG;
	}

}
