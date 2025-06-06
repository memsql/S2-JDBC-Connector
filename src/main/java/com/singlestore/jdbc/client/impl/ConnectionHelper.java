// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.SocketHelper;
import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.socket.impl.SocketHandlerFunction;
import com.singlestore.jdbc.client.socket.impl.SocketUtility;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.message.client.SslRequestPacket;
import com.singlestore.jdbc.message.server.AuthSwitchPacket;
import com.singlestore.jdbc.message.server.ErrorPacket;
import com.singlestore.jdbc.message.server.OkPacket;
import com.singlestore.jdbc.plugin.*;
import com.singlestore.jdbc.plugin.authentication.AuthenticationPluginLoader;
import com.singlestore.jdbc.plugin.tls.TlsSocketPluginLoader;
import com.singlestore.jdbc.util.ConfigurableSocketFactory;
import com.singlestore.jdbc.util.constants.Capabilities;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.List;
import javax.net.SocketFactory;
import javax.net.ssl.*;

/** Connection creation helper class */
public final class ConnectionHelper {

  private static final SocketHandlerFunction socketHandler;

  static {
    SocketHandlerFunction init;
    try {
      init = SocketUtility.getSocketHandler();
    } catch (Throwable t) {
      init = ConnectionHelper::standardSocket;
    }
    socketHandler = init;
  }

  /**
   * Create socket accordingly to options.
   *
   * @param conf Url options
   * @param hostAddress host ( mandatory but for named pipe / unix socket)
   * @return a nex socket
   * @throws IOException if connection error occur
   * @throws SQLException in case of configuration error
   */
  public static Socket createSocket(Configuration conf, HostAddress hostAddress)
      throws IOException, SQLException {
    return socketHandler.apply(conf, hostAddress);
  }

  /**
   * Use standard socket implementation.
   *
   * @param conf url options
   * @param hostAddress host to connect
   * @return socket
   * @throws IOException in case of error establishing socket.
   * @throws SQLException in case host is null
   */
  public static Socket standardSocket(Configuration conf, HostAddress hostAddress)
      throws IOException, SQLException {
    SocketFactory socketFactory;
    String socketFactoryName = conf.socketFactory();
    if (socketFactoryName != null) {
      try {
        @SuppressWarnings("unchecked")
        Class<SocketFactory> socketFactoryClass =
            (Class<SocketFactory>)
                Class.forName(socketFactoryName, false, ConnectionHelper.class.getClassLoader());
        if (!SocketFactory.class.isAssignableFrom(socketFactoryClass)) {
          throw new IOException(
              "Wrong Socket factory implementation '" + conf.socketFactory() + "'");
        }
        Constructor<? extends SocketFactory> constructor = socketFactoryClass.getConstructor();
        socketFactory = constructor.newInstance();
        if (socketFactory instanceof ConfigurableSocketFactory) {
          ((ConfigurableSocketFactory) socketFactory).setConfiguration(conf, hostAddress.host);
        }
        return socketFactory.createSocket();
      } catch (Exception exp) {
        throw new IOException(
            "Socket factory failed to initialized with option \"socketFactory\" set to \""
                + conf.socketFactory()
                + "\"",
            exp);
      }
    }
    socketFactory = SocketFactory.getDefault();
    return socketFactory.createSocket();
  }

  /**
   * Connect socket
   *
   * @param conf configuration
   * @param hostAddress host to connect
   * @return socket
   * @throws SQLException if hostname is required and not provided, or socket cannot be created
   */
  public static Socket connectSocket(final Configuration conf, final HostAddress hostAddress)
      throws SQLException {
    Socket socket;
    try {
      if (conf.pipe() == null && conf.localSocket() == null && hostAddress == null)
        throw new SQLException(
            "hostname must be set to connect socket if not using local socket or pipe");
      socket = createSocket(conf, hostAddress);
      SocketHelper.setSocketOption(conf, socket);
      if (!socket.isConnected()) {
        InetSocketAddress sockAddr =
            conf.pipe() == null && conf.localSocket() == null
                ? new InetSocketAddress(hostAddress.host, hostAddress.port)
                : null;
        socket.connect(sockAddr, conf.connectTimeout());
      }
      return socket;
    } catch (SocketTimeoutException ste) {
      throw new SQLTimeoutException(
          String.format("Socket timeout when connecting to %s. %s", hostAddress, ste.getMessage()),
          "08000",
          ste);
    } catch (IOException ioe) {
      throw new SQLNonTransientConnectionException(
          String.format(
              "Socket fail to connect to host:%s. %s",
              hostAddress == null ? conf.localSocket() : hostAddress, ioe.getMessage()),
          "08000",
          ioe);
    }
  }

  /**
   * Initialize client capability according to configuration and server capabilities.
   *
   * @param configuration configuration
   * @param serverCapabilities server capabilities
   * @return client capabilities
   */
  public static long initializeClientCapabilities(
      final Configuration configuration, final long serverCapabilities) {
    long capabilities = initializeBaseCapabilities();
    capabilities = applyOptionalCapabilities(capabilities, configuration);
    capabilities = applyTechnicalCapabilities(capabilities, configuration);
    capabilities = applyConnectionCapabilities(capabilities, configuration);

    return capabilities & serverCapabilities;
  }

  private static long initializeBaseCapabilities() {
    return Capabilities.IGNORE_SPACE
        | Capabilities.CLIENT_PROTOCOL_41
        | Capabilities.TRANSACTIONS
        | Capabilities.SECURE_CONNECTION
        | Capabilities.MULTI_RESULTS
        | Capabilities.PS_MULTI_RESULTS
        | Capabilities.PLUGIN_AUTH
        | Capabilities.CONNECT_ATTRS
        | Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA
        | Capabilities.CLIENT_SESSION_TRACK;
  }

  private static long applyOptionalCapabilities(long capabilities, Configuration configuration) {
    if (shouldEnableMetadataCache(configuration)) {
      capabilities |= Capabilities.CACHE_METADATA;
    }

    if (!configuration.useAffectedRows()) {
      capabilities |= Capabilities.FOUND_ROWS;
    }

    if (configuration.allowMultiQueries()) {
      capabilities |= Capabilities.MULTI_STATEMENTS;
    }

    if (configuration.allowLocalInfile()) {
      capabilities |= Capabilities.LOCAL_FILES;
    }
    return capabilities;
  }

  private static long applyTechnicalCapabilities(long capabilities, Configuration configuration) {
    // not used, hardcoded to false in SingleStore
    if (getBooleanProperty(configuration, "deprecateEof", true)) {
      capabilities |= Capabilities.CLIENT_DEPRECATE_EOF;
    }

    if (configuration.useCompression()) {
      capabilities |= Capabilities.COMPRESS;
    }

    return capabilities;
  }

  private static long applyConnectionCapabilities(long capabilities, Configuration configuration) {

    if (shouldConnectWithDb(configuration)) {
      capabilities |= Capabilities.CONNECT_WITH_DB;
    }

    if (shouldEnableSsl(configuration)) {
      capabilities |= Capabilities.SSL;
    }

    return capabilities;
  }

  private static boolean getBooleanProperty(
      Configuration configuration, String propertyName, boolean defaultValue) {
    return Boolean.parseBoolean(
        configuration.nonMappedOptions().getProperty(propertyName, String.valueOf(defaultValue)));
  }

  private static boolean shouldEnableMetadataCache(Configuration configuration) {
    return configuration.useServerPrepStmts()
        && getBooleanProperty(configuration, "enableSkipMeta", true);
  }

  private static boolean shouldConnectWithDb(Configuration configuration) {
    return configuration.database() != null && !configuration.createDatabaseIfNotExist();
  }

  private static boolean shouldEnableSsl(Configuration configuration) {
    return configuration.sslMode() != SslMode.DISABLE;
  }

  /**
   * Authentication swtich handler
   *
   * @param credential credential
   * @param writer socket writer
   * @param reader socket reader
   * @param context connection context
   * @throws IOException if any socket error occurs
   * @throws SQLException if any other kind of issue occurs
   */
  public static void authenticationHandler(
      Credential credential, Writer writer, Reader reader, Context context)
      throws SQLException, IOException {

    writer.permitTrace(true);
    Configuration conf = context.getConf();
    ReadableByteBuf buf = reader.readReusablePacket();

    authentication_loop:
    while (true) {
      switch (buf.getByte() & 0xFF) {
        case 0xFE:
          // *************************************************************************************
          // Authentication Switch Request see
          // https://mariadb.com/kb/en/library/connection/#authentication-switch-request
          // *************************************************************************************
          AuthSwitchPacket authSwitchPacket = AuthSwitchPacket.decode(buf);
          AuthenticationPluginFactory authPluginFactory =
              AuthenticationPluginLoader.get(authSwitchPacket.getPlugin(), conf);
          if (authPluginFactory.requireSsl() && !context.hasClientCapability(Capabilities.SSL)) {
            throw context
                .getExceptionFactory()
                .create(
                    "Cannot use authentication plugin "
                        + authPluginFactory.type()
                        + " if SSL is not enabled.",
                    "08000");
          }
          AuthenticationPlugin authenticationPlugin =
              authPluginFactory.initialize(
                  credential.getPassword(), authSwitchPacket.getSeed(), conf);
          buf = authenticationPlugin.process(writer, reader, context);
          break;

        case 0xFF:
          // *************************************************************************************
          // ERR_Packet
          // see https://mariadb.com/kb/en/library/err_packet/
          // *************************************************************************************
          ErrorPacket errorPacket = new ErrorPacket(buf, context);
          throw context
              .getExceptionFactory()
              .create(
                  errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

        case 0x00:
          // *************************************************************************************
          // OK_Packet -> Authenticated !
          // see https://mariadb.com/kb/en/library/ok_packet/
          // *************************************************************************************
          new OkPacket(buf, context);
          break authentication_loop;

        default:
          throw context
              .getExceptionFactory()
              .create(
                  "unexpected data during authentication (header=" + (buf.getUnsignedByte()),
                  "08000");
      }
    }
    writer.permitTrace(true);
  }

  /**
   * Load user/password plugin if configured to.
   *
   * @param credentialPlugin configuration credential plugin
   * @param configuration configuration
   * @param hostAddress current connection host address
   * @return credentials
   * @throws SQLException if configured credential plugin fail
   */
  public static Credential loadCredential(
      CredentialPlugin credentialPlugin, Configuration configuration, HostAddress hostAddress)
      throws SQLException {
    if (credentialPlugin != null) {
      return credentialPlugin.initialize(configuration, configuration.user(), hostAddress).get();
    }
    return new Credential(configuration.user(), configuration.password());
  }

  /**
   * Create SSL wrapper
   *
   * @param hostAddress host
   * @param socket socket
   * @param clientCapabilities client capabilities
   * @param exchangeCharset connection charset
   * @param context connection context
   * @param writer socket writer
   * @return SSLsocket
   * @throws IOException if any socket error occurs
   * @throws SQLException for any other kind of error
   */
  public static SSLSocket sslWrapper(
      final HostAddress hostAddress,
      final Socket socket,
      long clientCapabilities,
      final byte exchangeCharset,
      Context context,
      Writer writer)
      throws SQLException, IOException {

    Configuration conf = context.getConf();
    if (conf.sslMode() != SslMode.DISABLE) {

      if (!context.hasServerCapability(Capabilities.SSL)) {
        throw context
            .getExceptionFactory()
            .create("Trying to connect with ssl, but ssl not enabled in the server", "08000");
      }

      clientCapabilities |= Capabilities.SSL;
      SslRequestPacket.create(clientCapabilities, exchangeCharset).encode(writer, context);

      TlsSocketPlugin socketPlugin = TlsSocketPluginLoader.get(conf.tlsSocketType());
      SSLSocketFactory sslSocketFactory;
      TrustManager[] trustManagers =
          socketPlugin.getTrustManager(conf, context.getExceptionFactory());
      try {
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(
            socketPlugin.getKeyManager(conf, context.getExceptionFactory()), trustManagers, null);
        sslSocketFactory = sslContext.getSocketFactory();
      } catch (KeyManagementException keyManagementEx) {
        throw context
            .getExceptionFactory()
            .create("Could not initialize SSL context", "08000", keyManagementEx);
      } catch (NoSuchAlgorithmException noSuchAlgorithmEx) {
        throw context
            .getExceptionFactory()
            .create("SSLContext TLS Algorithm not unknown", "08000", noSuchAlgorithmEx);
      }
      SSLSocket sslSocket = socketPlugin.createSocket(socket, sslSocketFactory);

      enabledSslProtocolSuites(sslSocket, conf);
      enabledSslCipherSuites(sslSocket, conf);

      sslSocket.setUseClientMode(true);
      sslSocket.startHandshake();

      // perform hostname verification
      // (rfc2818 indicate that if "client has external information as to the expected identity of
      // the server, the hostname check MAY be omitted")
      if (conf.sslMode() == SslMode.VERIFY_FULL && hostAddress != null) {

        SSLSession session = sslSocket.getSession();
        try {
          socketPlugin.verify(hostAddress.host, session, context.getThreadId());
        } catch (SSLException ex) {
          throw context
              .getExceptionFactory()
              .create(
                  "SSL hostname verification failed : "
                      + ex.getMessage()
                      + "\nThis verification can be disabled using the sslMode to VERIFY_CA "
                      + "but won't prevent man-in-the-middle attacks anymore",
                  "08006");
        }
      }
      return sslSocket;
    }
    return null;
  }

  /**
   * Return possible protocols : values of option enabledSslProtocolSuites is set, or default to
   * "TLSv1,TLSv1.1".
   *
   * @param sslSocket current sslSocket
   * @throws SQLException if protocol isn't a supported protocol
   */
  static void enabledSslProtocolSuites(SSLSocket sslSocket, Configuration conf)
      throws SQLException {
    if (conf.enabledSslProtocolSuites() != null) {
      List<String> possibleProtocols = Arrays.asList(sslSocket.getSupportedProtocols());
      String[] protocols = conf.enabledSslProtocolSuites().split("[,;\\s]+");
      for (String protocol : protocols) {
        if (!possibleProtocols.contains(protocol)) {
          throw new SQLException(
              "Unsupported SSL protocol '"
                  + protocol
                  + "'. Supported protocols : "
                  + possibleProtocols.toString().replace("[", "").replace("]", ""));
        }
      }
      sslSocket.setEnabledProtocols(protocols);
    }
  }

  /**
   * Set ssl socket cipher according to options.
   *
   * @param sslSocket current ssl socket
   * @param conf configuration
   * @throws SQLException if a cipher isn't known
   */
  static void enabledSslCipherSuites(SSLSocket sslSocket, Configuration conf) throws SQLException {
    if (conf.enabledSslCipherSuites() != null) {
      List<String> possibleCiphers = Arrays.asList(sslSocket.getSupportedCipherSuites());
      String[] ciphers = conf.enabledSslCipherSuites().split("[,;\\s]+");
      for (String cipher : ciphers) {
        if (!possibleCiphers.contains(cipher)) {
          throw new SQLException(
              "Unsupported SSL cipher '"
                  + cipher
                  + "'. Supported ciphers : "
                  + possibleCiphers.toString().replace("[", "").replace("]", ""));
        }
      }
      sslSocket.setEnabledCipherSuites(ciphers);
    }
  }
}
