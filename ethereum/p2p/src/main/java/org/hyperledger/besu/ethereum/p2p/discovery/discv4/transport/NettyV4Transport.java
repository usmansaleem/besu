/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.transport;

import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryServiceException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.V4Transport;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.util.DecodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Netty-backed {@link V4Transport} that binds and owns its own UDP socket. */
public final class NettyV4Transport implements V4Transport {

  private static final Logger LOG = LoggerFactory.getLogger(NettyV4Transport.class);

  private final InetSocketAddress bindAddress;

  // Lazily created in start(), so a node that never starts this transport (e.g. discovery
  // disabled) doesn't pay for a permanently-idle event-loop thread.
  private volatile EventLoopGroup eventLoopGroup;

  private volatile NioDatagramChannel channel;
  private volatile InboundV4Handler inboundHandler;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private NettyV4Transport(final InetSocketAddress bindAddress) {
    this.bindAddress = bindAddress;
  }

  public static NettyV4Transport create(final InetSocketAddress bindAddress) {
    return new NettyV4Transport(bindAddress);
  }

  @Override
  public void setInboundHandler(final InboundV4Handler handler) {
    this.inboundHandler = handler;
  }

  @Override
  public CompletableFuture<InetSocketAddress> start() {
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("NettyV4Transport already started"));
    }

    this.eventLoopGroup =
        new MultiThreadIoEventLoopGroup(
            1, (ThreadFactory) r -> new Thread(r, "discv4-eventloop"), NioIoHandler.newFactory());

    final CompletableFuture<InetSocketAddress> future = new CompletableFuture<>();
    final Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(eventLoopGroup)
        // DiscV4 is IPv4-only; pin the family explicitly so 0.0.0.0 binds don't
        // open an IPv6 socket on Java 17+ (per Netty default).
        .channelFactory(
            (io.netty.channel.ChannelFactory<NioDatagramChannel>)
                () -> new NioDatagramChannel(SocketProtocolFamily.INET))
        .handler(
            new ChannelInitializer<NioDatagramChannel>() {
              @Override
              protected void initChannel(final NioDatagramChannel ch) {
                final ChannelPipeline pipeline = ch.pipeline();
                pipeline.addFirst(new LoggingHandler(LogLevel.TRACE));
                pipeline.addLast(new V4InboundHandler());
              }
            });

    final ChannelFuture bindFuture = bootstrap.bind(bindAddress);
    bindFuture.addListener(
        result -> {
          if (!result.isSuccess()) {
            eventLoopGroup.shutdownGracefully();
            Throwable cause = result.cause();
            if (cause instanceof BindException || cause instanceof SocketException) {
              cause =
                  new PeerDiscoveryServiceException(
                      String.format(
                          "Failed to bind Ethereum UDP discovery listener to %s:%d: %s",
                          bindAddress.getHostString(), bindAddress.getPort(), cause.getMessage()));
            }
            future.completeExceptionally(cause);
            return;
          }
          this.channel = (NioDatagramChannel) bindFuture.channel();
          final InetSocketAddress bound = channel.localAddress();
          LOG.info(
              "DiscV4 UDP transport started, listening on {}:{}",
              bound.getHostString(),
              bound.getPort());
          future.complete(bound);
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> send(final InetSocketAddress recipient, final Bytes data) {
    final NioDatagramChannel ch = this.channel;
    if (ch == null || !ch.isActive()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Transport is not started or already stopped"));
    }
    final CompletableFuture<Void> future = new CompletableFuture<>();
    ch.writeAndFlush(new DatagramPacket(Unpooled.wrappedBuffer(data.toArray()), recipient))
        .addListener(
            result -> {
              if (result.isSuccess()) {
                future.complete(null);
              } else {
                future.completeExceptionally(result.cause());
              }
            });
    return future;
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (!stopped.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    final EventLoopGroup group = this.eventLoopGroup;
    if (group == null) {
      // start() was never called (e.g. discovery disabled) - nothing to stop.
      return CompletableFuture.completedFuture(null);
    }
    final NioDatagramChannel ch = this.channel;
    if (ch == null || !ch.isOpen()) {
      return toFuture(group.shutdownGracefully());
    }
    return toFuture(ch.close()).thenCompose(v -> toFuture(group.shutdownGracefully()));
  }

  private static CompletableFuture<Void> toFuture(final io.netty.util.concurrent.Future<?> f) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();
    f.addListener(
        result -> {
          if (result.isSuccess()) {
            cf.complete(null);
          } else {
            cf.completeExceptionally(result.cause());
          }
        });
    return cf;
  }

  private final class V4InboundHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) {
      final InboundV4Handler handler = inboundHandler;
      if (handler == null) {
        return;
      }
      final InetSocketAddress sender = msg.sender();
      // Copy before SimpleChannelInboundHandler releases the buffer
      final byte[] bytes = new byte[msg.content().readableBytes()];
      msg.content().readBytes(bytes);
      handler.onPacket(sender, Bytes.wrap(bytes));
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      if (cause instanceof IOException || cause instanceof DecodeException) {
        LOG.debug("DiscV4 inbound handler exception", cause);
      } else {
        LOG.error("DiscV4 inbound handler exception", cause);
      }
    }
  }
}
