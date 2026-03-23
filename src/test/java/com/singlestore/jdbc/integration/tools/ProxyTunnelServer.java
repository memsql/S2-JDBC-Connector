// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 SingleStore, Inc.
package com.singlestore.jdbc.integration.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Lightweight in-process SOCKS5 proxy used in integration tests. */
public final class ProxyTunnelServer implements Closeable {

  private final ServerSocket serverSocket;
  private final ExecutorService acceptExecutor;
  private final ExecutorService tunnelExecutor;
  private final Set<Socket> activeSockets = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private volatile boolean closed;

  private ProxyTunnelServer() throws IOException {
    this.serverSocket = new ServerSocket();
    this.serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    this.acceptExecutor = Executors.newSingleThreadExecutor();
    this.tunnelExecutor = Executors.newCachedThreadPool();
  }

  public static ProxyTunnelServer startSocks5() throws IOException {
    ProxyTunnelServer server = new ProxyTunnelServer();
    server.start();
    return server;
  }

  public int getPort() {
    return serverSocket.getLocalPort();
  }

  private void start() {
    acceptExecutor.submit(
        () -> {
          while (!closed) {
            try {
              Socket client = serverSocket.accept();
              tunnelExecutor.submit(() -> handleClient(client));
            } catch (IOException e) {
              if (!closed) {
                throw new RuntimeException(e);
              }
              return;
            }
          }
        });
  }

  private void handleClient(Socket client) {
    activeSockets.add(client);
    try (Socket clientSocket = client) {
      handleSocks(clientSocket);
    } catch (IOException ignored) {
      // Test proxy should not fail the suite for one broken connection.
    } finally {
      activeSockets.remove(client);
    }
  }

  private void handleSocks(Socket clientSocket) throws IOException {
    BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
    BufferedOutputStream out = new BufferedOutputStream(clientSocket.getOutputStream());

    int version = in.read();
    if (version != 0x05) {
      throw new IOException("Unsupported SOCKS version: " + version);
    }
    int methodCount = in.read();
    if (methodCount < 0) {
      throw new IOException("Invalid SOCKS method count");
    }
    for (int i = 0; i < methodCount; i++) {
      if (in.read() < 0) {
        throw new IOException("Incomplete SOCKS methods");
      }
    }
    // No-authentication required.
    out.write(new byte[] {0x05, 0x00});
    out.flush();

    int reqVersion = in.read();
    int cmd = in.read();
    if (reqVersion != 0x05 || cmd != 0x01) {
      throw new IOException("Unsupported SOCKS request");
    }
    in.read(); // RSV
    int atyp = in.read();
    String host = readSocksAddress(in, atyp);
    int port = readUnsignedShort(in);

    Socket upstream;
    try {
      upstream = new Socket(host, port);
    } catch (IOException e) {
      // Host unreachable / connection refused – send SOCKS5 failure reply.
      out.write(new byte[] {0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0});
      out.flush();
      throw e;
    }
    try {
      // Success reply.
      out.write(new byte[] {0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0});
      out.flush();
      tunnel(clientSocket, in, out, upstream);
    } finally {
      upstream.close();
    }
  }

  private void tunnel(
      Socket clientSocket, InputStream clientIn, OutputStream clientOut, Socket upstream)
      throws IOException {
    activeSockets.add(upstream);
    try {
      InputStream upstreamIn = upstream.getInputStream();
      OutputStream upstreamOut = upstream.getOutputStream();

      tunnelExecutor.submit(
          () -> {
            pipe(clientIn, upstreamOut);
            // When client->upstream direction ends, close both to unblock the reverse direction.
            closeQuietly(clientSocket);
            closeQuietly(upstream);
          });
      pipe(upstreamIn, clientOut);
    } finally {
      closeQuietly(clientSocket);
      closeQuietly(upstream);
      activeSockets.remove(upstream);
    }
  }

  private static void pipe(InputStream in, OutputStream out) {
    byte[] buffer = new byte[8192];
    int read;
    try {
      while ((read = in.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
        out.flush();
      }
    } catch (IOException ignored) {
      // expected when one side closes.
    }
  }

  private String readSocksAddress(InputStream in, int atyp) throws IOException {
    switch (atyp) {
      case 0x01:
        byte[] ipv4 = readBytes(in, 4);
        return String.format(
            "%d.%d.%d.%d", ipv4[0] & 0xFF, ipv4[1] & 0xFF, ipv4[2] & 0xFF, ipv4[3] & 0xFF);
      case 0x03:
        int len = in.read();
        if (len < 0) {
          throw new IOException("Invalid SOCKS domain length");
        }
        return new String(readBytes(in, len), StandardCharsets.US_ASCII);
      case 0x04:
        byte[] ipv6 = readBytes(in, 16);
        return InetAddress.getByAddress(ipv6).getHostAddress();
      default:
        throw new IOException("Unsupported SOCKS address type: " + atyp);
    }
  }

  private static int readUnsignedShort(InputStream in) throws IOException {
    int high = in.read();
    int low = in.read();
    if (high < 0 || low < 0) {
      throw new IOException("Unexpected EOF reading unsigned short");
    }
    return (high << 8) | low;
  }

  private static byte[] readBytes(InputStream in, int len) throws IOException {
    byte[] out = new byte[len];
    int offset = 0;
    while (offset < len) {
      int read = in.read(out, offset, len - offset);
      if (read < 0) {
        throw new IOException("Unexpected EOF");
      }
      offset += read;
    }
    return out;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    serverSocket.close();
    // Close all active sockets to unblock threads stuck on blocking reads.
    for (Socket s : activeSockets) {
      closeQuietly(s);
    }
    acceptExecutor.shutdownNow();
    tunnelExecutor.shutdownNow();
  }

  private static void closeQuietly(Socket s) {
    try {
      s.close();
    } catch (IOException ignored) {
      // ignore
    }
  }
}
