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
import org.apache.flink.shaded.netty4.io.netty.bootstrap.*;
import org.apache.flink.shaded.netty4.io.netty.buffer.*;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.*;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.*;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.*;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.*;
import org.apache.flink.shaded.netty4.io.netty.channel.group.DefaultChannelGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.group.ChannelGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.unix.UnixChannelOption;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;
import java.util.concurrent.*;

public class MeTCPServer{

    private static final Logger LOG = LoggerFactory.getLogger(MeTCPServer.class);

    
    //TODO: Must configurable
    private final int port = 7000; //TCP server porta
    private static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE); //All Follower Channel
    private BlockingQueue<ByteBuf> queue; //Master BlockingQueue
    private int firstClient = 0;
    private final int meNum = 2;

    public MeTCPServer(BlockingQueue<ByteBuf> queue){
        this.queue = queue;
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
                ByteBuf data = queue.take();
                //channels.writeAndFlush(data.retainedDuplicate());
                for (Channel channel : channels) {
                    channel.writeAndFlush(data.retainedDuplicate());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}