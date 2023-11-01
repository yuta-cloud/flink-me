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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.bootstrap.*;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.nio.*;
import io.netty.channel.socket.nio.*;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.unix.UnixChannelOption;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.CharsetUtil;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.concurrent.*;

public class MeTCPServer{

    private static final Logger LOG = LoggerFactory.getLogger(MeTCPServer.class);

    
    //TODO: Must configurable
    private final int port = 7000; //TCP server porta
    private static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE); //All Follower Channel
    private BlockingQueue<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf> queue; //Master BlockingQueue
    private final AckReceiver ackReceiver;
    private int firstClient = 0;
    private final int meNum = 1;

    public MeTCPServer(BlockingQueue<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf> queue, AckReceiver receiver){
        this.queue = queue;
        this.ackReceiver = receiver;
    }
    
    public void run(){
        System.out.println("MeTCPServer start!");
        EventLoopGroup bossGroup = new EpollEventLoopGroup();
        EventLoopGroup workerGroup = new EpollEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup);
            b.channel(EpollServerSocketChannel.class);
            b.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {

                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                            byte[] receivedData = new byte[msg.readableBytes()];
                            msg.readBytes(receivedData);
                            LOG.debugs("Receive " + Arrays.toString(receivedData)));
                            if ("ACK".equals(Arrays.toString(receivedData))) {
                                ackReceiver.receiveAck();
                            }
                        }

                        @Override
                        public void channelActive(ChannelHandlerContext ctx) {
                            System.out.println("add client");
                            channels.add(ctx.channel()); //Add Follower to channels
                            firstClient++;
                            if(firstClient == meNum){
                                new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        processQueue();
                                    }
                                }).start();
                            }
                        }

                    });
                }
            })
            .option(EpollChannelOption.SO_REUSEPORT, true);

            ChannelFuture f = b.bind(port).sync();  // Bind and start to accept incoming connections.

            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    //send causal log to all client
    public void processQueue() {
        try {
            while (true) {
                org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf data = queue.take();
                io.netty.buffer.ByteBuf copiedMsg = io.netty.buffer.Unpooled.buffer();
                if (data.isReadable()) {
                    byte[] data_b = new byte[data.readableBytes()];
                    data.readBytes(data_b);
                    copiedMsg.writeBytes(data_b);
                }
                //channels.writeAndFlush(data.retainedDuplicate());
                for (Channel channel : channels) {
                    channel.writeAndFlush(copiedMsg);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}