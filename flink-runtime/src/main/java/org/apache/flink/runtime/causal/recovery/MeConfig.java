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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class MeConfig implements Serializable {
	private final Configuration config = new Configuration();

	public static final ConfigOption<String> ME_SERVER_ADDR = ConfigOptions
		.key("taskmanager.me.server.addr")
		.defaultValue("127.0.0.1")
		.withDescription("Set leader node addr");

	public static final ConfigOption<Integer> ME_SERVER_PORT = ConfigOptions
		.key("taskmanager.me.server.port")
		.defaultValue(7000)
		.withDescription("Set leader node port number");

	public static final ConfigOption<Boolean> ME_LEADER = ConfigOptions
		.key("taskmanager.me.server")
		.defaultValue(true)
		.withDescription("Is this node leader?");


	public String getServerAddr() {
		return config.getString(ME_SERVER_ADDR);
	}

	public int getServerPort() {
		return config.getInteger(ME_SERVER_PORT);
	}

	public boolean isLeader() {
		return config.getBoolean(ME_LEADER);
	}

	public void setServerAddr(String newAddr){
		config.setString(ME_SERVER_ADDR, newAddr);
	}



	@Override
	public String toString() {
		return "MeConfig{"
			+ "Server addr: " + getServerAddr()
			+ ", Server Port: " +getServerPort()
			+ ", Leader? " + isLeader()
			+ "}";
	}
}
