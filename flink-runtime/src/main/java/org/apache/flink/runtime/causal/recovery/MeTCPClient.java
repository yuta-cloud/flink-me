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

import org.apache.flink.shaded.netty4.io.netty.bootstrap.*;
import org.apache.flink.shaded.netty4.io.netty.buffer.*;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.*;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.*;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.*;

import java.util.concurrent.*;

public class MeTCPClient{
    //TODO: Must configurable
    private final int serverPort; //TCP server port
    private final String serverAddr; //TCP server addr
    private final BlockingQueue<ByteBuf> queue;
    private final MeConfig config;

    public MeTCPClient(BlockingQueue<ByteBuf> queue, MeConfig config){
        this.queue = queue;
        this.config = config;
        serverAddr = config.getServerAddr();
        serverPort = config.getServerPort();
    }

    public void run(){
        System.out.println("MeTCPClient start!");
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {

                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                            // Put ByteBuf to BlockingQueue
                            queue.put(msg.copy());
                        }
                    });
                }
            });

            // Start the client.
            ChannelFuture f = bootstrap.connect(serverAddr, serverPort).sync(); // Use the correct IP and port

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            workerGroup.shutdownGracefully();
        }
    }
}