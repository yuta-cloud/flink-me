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

import io.netty.bootstrap.*;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.nio.*;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;


import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.List;

public class MeTCPClient{
    //TODO: Must configurable
    private final int serverPort; //TCP server port
    private final String serverAddr; //TCP server addr
    private final BlockingQueue<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf> queue;
    private final MeConfig config;

    public MeTCPClient(BlockingQueue<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf> queue, MeConfig config){
        this.queue = queue;
        this.config = config;
        serverAddr = config.getServerAddr();
        serverPort = config.getServerPort();
    }

    public void run(){
        System.out.println("MeTCPClient start!");
        EventLoopGroup workerGroup = new EpollEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(EpollSocketChannel.class);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<io.netty.buffer.ByteBuf>() {

                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, io.netty.buffer.ByteBuf msg) throws Exception {
                            // Put ByteBuf to BlockingQueue
                            //io.netty.buffer.ByteBuf copiedMsg = msg.copy();
                            org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf copiedMsg = org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.buffer();
                            if (msg.isReadable()) {
                                byte[] data = new byte[msg.readableBytes()];
                                msg.readBytes(data);
                                copiedMsg.writeBytes(data);
                            }
                            queue.put(copiedMsg);
                            //msg.release();
                        }
                    });
                }

            });

            /*
            // Start the client.
            ChannelFuture f = bootstrap.connect(serverAddr, serverPort).sync(); // Use the correct IP and port

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
            */

           List<Channel> channels = new ArrayList<>();

            for (int i = 0; i < 1; i++) {
                ChannelFuture future = bootstrap.connect(serverAddr, serverPort).sync();
                if (future.isSuccess()) {
                    channels.add(future.channel());
                    System.out.println("Connected to server " + (i + 1));
                } else {
                    i--;
                }
            }

            for (Channel channel : channels) {
                channel.closeFuture().sync();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            workerGroup.shutdownGracefully();
        }
    }
}