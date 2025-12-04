package com.arth.sakimq.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * For broker
 */
public class NettyServer {

    private final int port;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        /* Recommended way in Netty 4.2: https://netty.io/wiki/netty-4.2-migration-guide.html */
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(
                0,  // default thread num: 2 * CPU cores
                NioIoHandler.newFactory()
        );

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();

                            // 添加编解码器
                            pipeline.addLast("decoder", new StringDecoder(CharsetUtil.UTF_8));
                            pipeline.addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));

                            // 添加业务处理器
                            pipeline.addLast("serverHandler", new SimpleChannelInboundHandler<String>() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    System.out.println("Client connected: " + ctx.channel().remoteAddress());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                    System.out.println("Client disconnected: " + ctx.channel().remoteAddress());
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
                                    System.out.println("Received from client: " + msg);

                                    // 简单回显作为响应示例
                                    ctx.writeAndFlush("Server response to: " + msg);
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            serverChannel = bootstrap.bind(port).sync().channel();
            System.out.println("Server started on port " + port);

            // 等待服务器通道关闭
            serverChannel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void stop() {
        if (serverChannel != null) serverChannel.close();
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }
}