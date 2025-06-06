// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.BaseContext;
import com.singlestore.jdbc.client.context.RedoContext;
import com.singlestore.jdbc.client.result.Result;
import com.singlestore.jdbc.client.result.StreamingResult;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.socket.impl.CompressInputStream;
import com.singlestore.jdbc.client.socket.impl.CompressOutputStream;
import com.singlestore.jdbc.client.socket.impl.PacketReader;
import com.singlestore.jdbc.client.socket.impl.PacketWriter;
import com.singlestore.jdbc.client.socket.impl.ReadAheadBufferedStream;
import com.singlestore.jdbc.client.util.ClosableLock;
import com.singlestore.jdbc.client.util.MutableByte;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.export.MaxAllowedPacketException;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.message.client.ClosePreparePacket;
import com.singlestore.jdbc.message.client.HandshakeResponse;
import com.singlestore.jdbc.message.client.QueryPacket;
import com.singlestore.jdbc.message.client.QuitPacket;
import com.singlestore.jdbc.message.server.ErrorPacket;
import com.singlestore.jdbc.message.server.InitialHandshakePacket;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import com.singlestore.jdbc.plugin.Credential;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import com.singlestore.jdbc.plugin.authentication.addon.ClearPasswordPluginFactory;
import com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialPlugin;
import com.singlestore.jdbc.util.Security;
import com.singlestore.jdbc.util.Version;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import javax.net.ssl.SSLSocket;

public class StandardClient implements Client, AutoCloseable {

  protected final ExceptionFactory exceptionFactory;
  private Socket socket;
  private final MutableByte sequence = new MutableByte();
  private final MutableByte compressionSequence = new MutableByte();
  private final ClosableLock lock;
  private final Configuration conf;
  private final HostAddress hostAddress;
  private final boolean disablePipeline;
  protected Writer writer;
  protected Context context;
  private boolean closed = false;
  private PacketReader reader;
  private com.singlestore.jdbc.Statement streamStmt = null;
  private ClientMessage streamMsg = null;
  private int socketTimeout;
  private BigInteger aggregatorId;
  private BigInteger initialSqlSelectLimit;
  private Version singleStoreVersion;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param hostAddress host
   * @param lock thread locker
   * @param skipPostCommands must connection post command be skipped
   * @throws SQLException if connection fails
   */
  @SuppressWarnings({"this-escape"})
  public StandardClient(
      Configuration conf, HostAddress hostAddress, ClosableLock lock, boolean skipPostCommands)
      throws SQLException {

    this.conf = conf;
    this.lock = lock;
    this.hostAddress = hostAddress;
    this.exceptionFactory = new ExceptionFactory(conf, hostAddress);
    this.disablePipeline = conf.disablePipeline();
    this.socketTimeout = conf.socketTimeout();

    String host = hostAddress != null ? hostAddress.host : null;

    try {
      connect(host, skipPostCommands);
    } catch (SQLInvalidAuthorizationSpecException sqlException) {
      // retry when connecting via browser auth token because token might have
      // expired while we were connecting or the cached token was wrong
      // error 2628 is JWT_TOKEN_EXPIRED
      // error 1045 is ACCESS_DENIED_ERROR
      if (conf.credentialPlugin() != null
              && conf.credentialPlugin().type().contains("BROWSER_SSO")
              && sqlException.getErrorCode() == 1045
          || sqlException.getErrorCode() == 2628) {
        BrowserCredentialPlugin credPlugin = (BrowserCredentialPlugin) conf.credentialPlugin();
        // clear both local cache and keyring to force re-acquiring the token
        Loggers.getLogger(StandardClient.class)
            .debug("Failed to connect with the JWT, retrying browser auth");
        credPlugin.clearKeyring();
        credPlugin.clearLocalCache();
        this.closed = false;
        connect(host, skipPostCommands);
      } else {
        throw sqlException;
      }
    }
  }

  private void connect(String host, boolean skipPostCommands) throws SQLException {
    this.socket = ConnectionHelper.connectSocket(conf, hostAddress);
    try {
      // **********************************************************************
      // creating socket
      // **********************************************************************
      OutputStream out = socket.getOutputStream();
      InputStream in =
          conf.useReadAheadInput()
              ? new ReadAheadBufferedStream(socket.getInputStream())
              : new BufferedInputStream(socket.getInputStream(), 16384);

      assignStream(out, in, conf, null);
      configureTimeout();
      InitialHandshakePacket handshake = handleServerHandshake();
      long clientCapabilities = setupClientCapabilities(handshake);
      // **********************************************************************
      // changing to SSL socket if needed
      // **********************************************************************
      SSLSocket sslSocket =
          ConnectionHelper.sslWrapper(
              hostAddress,
              socket,
              clientCapabilities,
              (byte) handshake.getDefaultCollation(),
              context,
              writer);

      if (sslSocket != null) {
        out = new BufferedOutputStream(sslSocket.getOutputStream(), 16384);
        in =
            conf.useReadAheadInput()
                ? new ReadAheadBufferedStream(sslSocket.getInputStream())
                : new BufferedInputStream(sslSocket.getInputStream(), 16384);
        assignStream(out, in, conf, handshake.getThreadId());
      }
      handleAuthentication(handshake, clientCapabilities);
      setupCompression(in, out, clientCapabilities, handshake.getThreadId());
      // **********************************************************************
      // post queries
      // **********************************************************************
      if (!skipPostCommands) {
        postConnectionQueries();
      }
      setSocketTimeout(conf.socketTimeout());
    } catch (SQLException sqlException) {
      destroySocket();
      throw sqlException;
    } catch (SocketTimeoutException ste) {
      destroySocket();
      throw new SQLTimeoutException(
          String.format("Socket timeout when connecting to %s. %s", hostAddress, ste.getMessage()),
          "08000",
          ste);
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.create(
          String.format("Could not connect to %s : %s", hostAddress, ioException.getMessage()),
          "08000",
          ioException);
    }
  }

  private void setupCompression(
      InputStream in, OutputStream out, long clientCapabilities, long threadId) {
    // **********************************************************************
    // activate compression if required
    // **********************************************************************
    if ((clientCapabilities & Capabilities.COMPRESS) != 0) {
      assignStream(
          new CompressOutputStream(out, compressionSequence),
          new CompressInputStream(in, compressionSequence),
          conf,
          threadId);
    }
  }

  private void assignStream(OutputStream out, InputStream in, Configuration conf, Long threadId) {
    this.writer =
        new PacketWriter(
            out, conf.maxQuerySizeToLog(), conf.maxAllowedPacket(), sequence, compressionSequence);
    this.writer.setServerThreadId(threadId, hostAddress);

    this.reader = new PacketReader(in, conf, sequence);
    this.reader.setServerThreadId(threadId, hostAddress);
  }

  private void configureTimeout() throws SQLException {
    if (conf.connectTimeout() > 0) {
      setSocketTimeout(conf.connectTimeout());
    } else if (conf.socketTimeout() > 0) {
      setSocketTimeout(conf.socketTimeout());
    }
  }

  private InitialHandshakePacket handleServerHandshake() throws SQLException, IOException {
    // read server handshake
    ReadableByteBuf buf =
        reader.readReusablePacket(Loggers.getLogger(StandardClient.class).isTraceEnabled());
    if (buf.getByte() == -1) {
      throwHandshakeError(buf);
    }
    return InitialHandshakePacket.decode(buf);
  }

  private void throwHandshakeError(ReadableByteBuf buf) throws SQLException {
    ErrorPacket errorPacket = new ErrorPacket(buf, null);
    throw this.exceptionFactory.create(
        errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
  }

  private long setupClientCapabilities(InitialHandshakePacket handshake) {
    this.exceptionFactory.setThreadId(handshake.getThreadId());
    long capabilities =
        ConnectionHelper.initializeClientCapabilities(conf, handshake.getCapabilities());

    initializeContext(handshake, capabilities);
    this.reader.setServerThreadId(handshake.getThreadId(), hostAddress);
    this.writer.setServerThreadId(handshake.getThreadId(), hostAddress);
    return capabilities;
  }

  private void initializeContext(InitialHandshakePacket handshake, long clientCapabilities) {
    PrepareCache cache =
        conf.cachePrepStmts() ? new PrepareCache(conf.prepStmtCacheSize(), this) : null;
    this.context =
        conf.transactionReplay()
            ? new RedoContext(
                hostAddress, handshake, clientCapabilities, conf, exceptionFactory, cache)
            : new BaseContext(
                hostAddress, handshake, clientCapabilities, conf, exceptionFactory, cache);
  }

  private void handleAuthentication(InitialHandshakePacket handshake, long clientCapabilities)
      throws IOException, SQLException {
    String authType = determineAuthType(handshake);
    Credential credential =
        ConnectionHelper.loadCredential(conf.credentialPlugin(), conf, hostAddress);

    sendHandshakeResponse(handshake, clientCapabilities, credential, authType);
    writer.flush();
    ConnectionHelper.authenticationHandler(credential, writer, reader, context);
  }

  private String determineAuthType(InitialHandshakePacket handshake) throws SQLException {
    // **********************************************************************
    // handling authentication
    // **********************************************************************
    String authType = handshake.getAuthenticationPluginType();
    CredentialPlugin credentialPlugin = conf.credentialPlugin();
    if (credentialPlugin != null && credentialPlugin.defaultAuthenticationPluginType() != null) {
      authType = credentialPlugin.defaultAuthenticationPluginType();
    }
    if (ClearPasswordPluginFactory.TYPE.equals(authType)
        && !context.hasClientCapability(Capabilities.SSL)) {
      throw context
          .getExceptionFactory()
          .create(
              "Cannot use authentication plugin "
                  + ClearPasswordPluginFactory.TYPE
                  + " if SSL is not enabled.",
              "08000");
    }
    return authType;
  }

  private void sendHandshakeResponse(
      InitialHandshakePacket handshake,
      long clientCapabilities,
      Credential credential,
      String authType)
      throws IOException {
    new HandshakeResponse(
            credential,
            authType,
            context.getSeed(),
            conf,
            hostAddress.host,
            clientCapabilities,
            (byte) handshake.getDefaultCollation())
        .encode(writer, context);
  }

  /** Closing socket in case of Connection error after socket creation. */
  protected void destroySocket() {
    closed = true;
    try {
      this.reader.close();
    } catch (IOException ee) {
      // eat exception
    }
    try {
      this.writer.close();
    } catch (IOException ee) {
      // eat exception
    }
    try {
      this.socket.close();
    } catch (IOException ee) {
      // eat exception
    }
  }

  private void postConnectionQueries() throws SQLException {
    List<String> commands = new ArrayList<>();
    int resInd = 0;
    if (conf.sessionVariables() != null) {
      commands.add("set " + Security.parseSessionVariables(conf.sessionVariables()));
      resInd++;
    }
    if (conf.autocommit() != null) {
      commands.add("set autocommit=" + (conf.autocommit() ? "true" : "false"));
      resInd++;
    }
    if (conf.database() != null && conf.createDatabaseIfNotExist()) {
      String escapedDb = conf.database().replace("`", "``");
      commands.add(String.format("CREATE DATABASE IF NOT EXISTS `%s`", escapedDb));
      commands.add(String.format("USE `%s`", escapedDb));
      resInd += 2;
    }
    if (conf.initSql() != null) {
      commands.add(conf.initSql());
      resInd++;
    }
    if (conf.nonMappedOptions().containsKey("initSql")) {
      String[] initialCommands = conf.nonMappedOptions().get("initSql").toString().split(";");
      commands.addAll(Arrays.asList(initialCommands));
      resInd += initialCommands.length;
    }
    commands.add(
        "SELECT @@max_allowed_packet, @@aggregator_id, @@sql_select_limit, @@memsql_version");
    try {
      List<Completion> res;
      ClientMessage[] msgs = new ClientMessage[commands.size()];
      for (int i = 0; i < commands.size(); i++) {
        msgs[i] = new QueryPacket(commands.get(i));
      }
      res =
          executePipeline(
              msgs,
              null,
              0,
              0L,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.TYPE_FORWARD_ONLY,
              false,
              true);

      // read max allowed packet
      Result result = (Result) res.get(resInd);
      if (result.next()) {
        this.writer.setMaxAllowedPacket(result.getInt(1));
        this.aggregatorId = result.getBigInteger(2);
        this.initialSqlSelectLimit = result.getBigInteger(3);
        this.singleStoreVersion = new Version(result.getString(4));
      }
      initializeExtendedTypesMetadata();
    } catch (SQLException sqlException) {
      throw exceptionFactory.create("Initialization command fail", "08000", sqlException);
    }
  }

  /**
   * Enables extended types metadata and vector output format if the appropriate configuration is
   * set.
   *
   * <p>Note: This feature is available starting from SingleStore version 8.7.1.
   */
  private void initializeExtendedTypesMetadata() throws SQLException {
    if (singleStoreVersion != null && singleStoreVersion.versionGreaterOrEqual(8, 7, 1)) {
      List<String> commands = new ArrayList<>();
      commands.add(
          "SET @@SESSION.enable_extended_types_metadata="
              + (conf.enableExtendedDataTypes() ? "on" : "off"));
      if (conf.vectorTypeOutputFormat() != null) {
        commands.add(
            String.format(
                "SET @@SESSION.vector_type_project_format=%s", conf.vectorTypeOutputFormat()));
      }
      ClientMessage[] msgs = new ClientMessage[commands.size()];
      for (int i = 0; i < commands.size(); i++) {
        msgs[i] = new QueryPacket(commands.get(i));
      }
      executePipeline(
          msgs, null, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false, true);
    } else if (conf.enableExtendedDataTypes() || conf.vectorTypeOutputFormat() != null) {
      Loggers.getLogger(StandardClient.class)
          .warn(
              "Configuration parameters: 'enableExtendedDataTypes' and 'vectorTypeOutputFormat' are not supported for SingleStore versions earlier than 8.7.1.");
    }
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }
  }

  /**
   * Send client message to server
   *
   * @param message client message
   * @return number of command send
   * @throws SQLException if socket error occurs
   */
  public int sendQuery(ClientMessage message) throws SQLException {
    checkNotClosed();
    try {
      Logger logger = Loggers.getLogger(StandardClient.class);
      if (logger.isDebugEnabled() && message.description() != null) {
        logger.debug("execute query: {}", message.description());
      }
      return message.encode(writer, context);
    } catch (IOException ioException) {
      if (ioException instanceof MaxAllowedPacketException) {
        if (((MaxAllowedPacketException) ioException).isMustReconnect()) {
          destroySocket();
          throw exceptionFactory
              .withSql(message.description())
              .create(
                  "Packet too big for current server max_allowed_packet value",
                  "08000",
                  ioException);
        }
        throw exceptionFactory
            .withSql(message.description())
            .create(
                "Packet too big for current server max_allowed_packet value", "HZ000", ioException);
      }
      destroySocket();
      throw exceptionFactory
          .withSql(message.description())
          .create("Socket error", "08000", ioException);
    }
  }

  @Override
  public List<Completion> execute(ClientMessage message, boolean canRedo) throws SQLException {
    return execute(
        message,
        null,
        0,
        0L,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.TYPE_FORWARD_ONLY,
        false,
        canRedo);
  }

  @Override
  public List<Completion> execute(
      ClientMessage message, com.singlestore.jdbc.Statement stmt, boolean canRedo)
      throws SQLException {
    return execute(
        message,
        stmt,
        0,
        0L,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.TYPE_FORWARD_ONLY,
        false,
        canRedo);
  }

  @Override
  public List<Completion> executePipeline(
      ClientMessage[] messages,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    List<Completion> results = new ArrayList<>();
    int perMsgCounter = 0;
    int readCounter = 0;
    int[] responseMsg = new int[messages.length];
    try {
      if (disablePipeline) {
        for (readCounter = 0; readCounter < messages.length; readCounter++) {
          results.addAll(
              execute(
                  messages[readCounter],
                  stmt,
                  fetchSize,
                  maxRows,
                  resultSetConcurrency,
                  resultSetType,
                  closeOnCompletion,
                  canRedo));
        }
      } else {
        for (int i = 0; i < messages.length; i++) {
          responseMsg[i] = sendQuery(messages[i]);
        }
        while (readCounter < messages.length) {
          readCounter++;
          for (perMsgCounter = 0; perMsgCounter < responseMsg[readCounter - 1]; perMsgCounter++) {
            results.addAll(
                readResponse(
                    stmt,
                    messages[readCounter - 1],
                    fetchSize,
                    maxRows,
                    resultSetConcurrency,
                    resultSetType,
                    closeOnCompletion));
          }
        }
      }
      return results;
    } catch (SQLException sqlException) {
      if (!closed) {
        results.add(null);
        // read remaining results
        perMsgCounter++;
        for (; readCounter > 0 && perMsgCounter < responseMsg[readCounter - 1]; perMsgCounter++) {
          try {
            results.addAll(
                readResponse(
                    stmt,
                    messages[readCounter - 1],
                    fetchSize,
                    maxRows,
                    resultSetConcurrency,
                    resultSetType,
                    closeOnCompletion));
          } catch (SQLException e) {
            // eat
          }
        }

        for (int i = readCounter; i < messages.length; i++) {
          for (int j = 0; j < responseMsg[i]; j++) {
            try {
              results.addAll(
                  readResponse(
                      stmt,
                      messages[i],
                      fetchSize,
                      maxRows,
                      resultSetConcurrency,
                      resultSetType,
                      closeOnCompletion));
            } catch (SQLException e) {
              results.add(null);
            }
          }
        }

        // prepare associated to PrepareStatement need to be uncached
        for (Completion result : results) {
          if (result instanceof PrepareResultPacket && stmt instanceof ServerPreparedStatement) {
            try {
              ((PrepareResultPacket) result).decrementUse(this, (ServerPreparedStatement) stmt);
            } catch (SQLException e) {
              // eat
            }
          }
        }
      }

      int batchUpdateLength = 0;
      for (ClientMessage message : messages) {
        batchUpdateLength += message.batchUpdateLength();
      }
      throw exceptionFactory.createBatchUpdate(
          results, batchUpdateLength, responseMsg, sqlException);
    }
  }

  @Override
  public List<Completion> execute(
      ClientMessage message,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    List<Completion> completions =
        executeInternal(
            message,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion);
    return completions;
  }

  private List<Completion> executeInternal(
      ClientMessage message,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    int nbResp = sendQuery(message);
    if (nbResp == 1) {
      return readResponse(
          stmt,
          message,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
    } else {
      if (streamStmt != null) {
        streamStmt.fetchRemaining();
        streamStmt = null;
      }
      List<Completion> completions = new ArrayList<>();
      try {
        while (nbResp-- > 0) {
          readResults(
              stmt,
              message,
              completions,
              fetchSize,
              maxRows,
              resultSetConcurrency,
              resultSetType,
              closeOnCompletion);
        }
        return completions;
      } catch (SQLException e) {
        while (nbResp-- > 0) {
          try {
            readResults(
                stmt,
                message,
                completions,
                fetchSize,
                maxRows,
                resultSetConcurrency,
                resultSetType,
                closeOnCompletion);
          } catch (SQLException ee) {
            // eat
          }
        }
        throw e;
      }
    }
  }

  /**
   * Read server responses for a client message
   *
   * @param stmt statement that issue the message
   * @param message client message sent
   * @param fetchSize fetch size
   * @param maxRows maximum number of rows
   * @param resultSetConcurrency concurrency
   * @param resultSetType result-set type
   * @param closeOnCompletion close statement on resultset completion
   * @return list of result
   * @throws SQLException if any error occurs
   */
  public List<Completion> readResponse(
      com.singlestore.jdbc.Statement stmt,
      ClientMessage message,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    checkNotClosed();
    if (streamStmt != null) {
      streamStmt.fetchRemaining();
      streamStmt = null;
    }
    List<Completion> completions = new ArrayList<>();
    readResults(
        stmt,
        message,
        completions,
        fetchSize,
        maxRows,
        resultSetConcurrency,
        resultSetType,
        closeOnCompletion);
    return completions;
  }

  /**
   * Read server response
   *
   * @param message client message that was sent
   * @throws SQLException if any error occurs
   */
  public void readResponse(ClientMessage message) throws SQLException {
    checkNotClosed();
    if (streamStmt != null) {
      streamStmt.fetchRemaining();
      streamStmt = null;
    }
    List<Completion> completions = new ArrayList<>();
    readResults(
        null,
        message,
        completions,
        0,
        0L,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.TYPE_FORWARD_ONLY,
        false);
  }

  @Override
  public void closePrepare(Prepare prepare) throws SQLException {
    checkNotClosed();
    try {
      new ClosePreparePacket(prepare.getStatementId()).encode(writer, context);
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.create(
          "Socket error during post connection queries: " + ioException.getMessage(),
          "08000",
          ioException);
    }
  }

  @Override
  public void readStreamingResults(
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    if (streamStmt != null) {
      readResults(
          streamStmt,
          streamMsg,
          completions,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
    }
  }

  private void readResults(
      com.singlestore.jdbc.Statement stmt,
      ClientMessage message,
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    completions.add(
        readPacket(
            stmt,
            message,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion));

    while ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
      completions.add(
          readPacket(
              stmt,
              message,
              fetchSize,
              maxRows,
              resultSetConcurrency,
              resultSetType,
              closeOnCompletion));
    }
  }

  /**
   * Read a MySQL packet from socket
   *
   * @param message client message issuing the result
   * @return a mysql result
   * @throws SQLException if any error occurs
   */
  public Completion readPacket(ClientMessage message) throws SQLException {
    return readPacket(
        null, message, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  /**
   * Read server response packet.
   *
   * @param stmt current statement (null if internal)
   * @param message current message
   * @param fetchSize default fetch size
   * @param resultSetConcurrency concurrency
   * @param resultSetType type
   * @param maxRows max rows
   * @param closeOnCompletion must resultset close statement on completion
   * @return completion result
   * @throws SQLException if any exception
   */
  public Completion readPacket(
      com.singlestore.jdbc.Statement stmt,
      ClientMessage message,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    try {
      boolean traceEnable = Loggers.getLogger(StandardClient.class).isTraceEnabled();
      Completion completion =
          message.readPacket(
              stmt,
              fetchSize,
              maxRows,
              resultSetConcurrency,
              resultSetType,
              closeOnCompletion,
              reader,
              writer,
              context,
              exceptionFactory,
              lock,
              traceEnable,
              message);
      if (completion instanceof StreamingResult && !((StreamingResult) completion).loaded()) {
        streamStmt = stmt;
        streamMsg = message;
      }
      return completion;
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory
          .withSql(message.description())
          .create(
              ioException instanceof SocketTimeoutException
                  ? "Socket timeout error"
                  : "Socket error",
              "08000",
              ioException);
    }
  }

  /**
   * Throw an exception if client is closed
   *
   * @throws SQLException if closed
   */
  protected void checkNotClosed() throws SQLException {
    if (closed) {
      throw exceptionFactory.create("Connection is closed", "08000", 1220);
    }
  }

  private void closeSocket() {
    try {
      try {
        long maxCurrentMillis = System.currentTimeMillis() + 10;
        socket.shutdownOutput();
        socket.setSoTimeout(3);
        InputStream is = socket.getInputStream();
        //noinspection StatementWithEmptyBody
        while (is.read() != -1 && System.currentTimeMillis() < maxCurrentMillis) {
          // read byte
        }
      } catch (Throwable t) {
        // eat exception
      }
      writer.close();
      reader.close();
    } catch (IOException e) {
      // eat
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        // socket closed, if any error, so not throwing error
      }
    }
  }

  public boolean isClosed() {
    return closed;
  }

  public Context getContext() {
    return context;
  }

  public void abort(Executor executor) throws SQLException {
    if (executor == null) {
      throw exceptionFactory.create("Cannot abort the connection: null executor passed");
    }

    //    fireConnectionClosed(new ConnectionEvent(this));
    boolean lockStatus = lock.tryLock();

    if (!this.closed) {
      this.closed = true;
      Loggers.getLogger(StandardClient.class)
          .debug("aborting connection {}", context.getThreadId());
      if (!lockStatus) {
        // lock not available : query is running
        // force end by executing an KILL connection
        try (StandardClient cli = new StandardClient(conf, hostAddress, new ClosableLock(), true)) {
          cli.execute(new QueryPacket("KILL " + context.getThreadId()), false);
        } catch (SQLException e) {
          // eat
        }
      } else {
        try {
          QuitPacket.INSTANCE.encode(writer, context);
        } catch (IOException e) {
          // eat
        }
      }
      if (streamStmt != null) {
        streamStmt.abort();
      }
      closeSocket();
    }

    if (lockStatus) {
      lock.unlock();
    }
  }

  public int getSocketTimeout() {
    return this.socketTimeout;
  }

  public void setSocketTimeout(int milliseconds) throws SQLException {
    try {
      socketTimeout = milliseconds;
      socket.setSoTimeout(milliseconds);
    } catch (SocketException se) {
      throw exceptionFactory.create("Cannot set the network timeout", "42000", se);
    }
  }

  public void close() throws SQLException {
    boolean locked = lock.tryLock();

    if (!this.closed) {
      this.closed = true;
      try {
        QuitPacket.INSTANCE.encode(writer, context);
      } catch (IOException e) {
        // eat
      }
      closeSocket();
    }

    if (locked) {
      lock.unlock();
    }
  }

  @Override
  public String getSocketIp() {
    return this.socket.getInetAddress() == null
        ? null
        : this.socket.getInetAddress().getHostAddress();
  }

  public ExceptionFactory getExceptionFactory() {
    return exceptionFactory;
  }

  public HostAddress getHostAddress() {
    return hostAddress;
  }

  @Override
  public BigInteger getAggregatorId() {
    return this.aggregatorId;
  }

  @Override
  public BigInteger getInitialSqlSelectLimit() {
    return this.initialSqlSelectLimit;
  }

  @Override
  public Version getSingleStoreVersion() {
    return singleStoreVersion;
  }

  public void reset() {
    context.resetStateFlag();
    context.resetPrepareCache();
  }
}
