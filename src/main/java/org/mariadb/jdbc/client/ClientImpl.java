/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLSocket;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.context.BaseContext;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.context.RedoContext;
import org.mariadb.jdbc.client.result.Result;
import org.mariadb.jdbc.client.result.StreamingResult;
import org.mariadb.jdbc.message.client.*;
import org.mariadb.jdbc.message.server.Completion;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.plugin.credential.Credential;
import org.mariadb.jdbc.plugin.credential.CredentialPlugin;
import org.mariadb.jdbc.util.MutableInt;
import org.mariadb.jdbc.util.Security;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.HaMode;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public class ClientImpl implements Client, AutoCloseable {

  private static final Logger logger = Loggers.getLogger(ClientImpl.class);

  private static Integer MAX_ALLOWED_PACKET = 0;

  private final Socket socket;
  private final Context context;
  private final MutableInt sequence = new MutableInt(0);
  private final ReentrantLock lock;
  private final Configuration conf;
  private final HostAddress hostAddress;
  private boolean closed = false;
  private ExceptionFactory exceptionFactory;
  private PacketWriter writer;
  private PacketReader reader;
  private OutputStream out;
  private org.mariadb.jdbc.Statement streamStmt = null;
  private ClientMessage streamMsg = null;
  private int socketTimeout;

  public ClientImpl(
      Configuration conf,
      HostAddress hostAddress,
      boolean saveTransaction,
      ReentrantLock lock,
      boolean skipPostCommands)
      throws SQLException {
    this.conf = conf;
    this.lock = lock;
    this.hostAddress = hostAddress;
    this.exceptionFactory = new ExceptionFactory(conf, hostAddress);

    List<String> galeraAllowedStates =
        conf.galeraAllowedState() == null
            ? Collections.emptyList()
            : Arrays.asList(conf.galeraAllowedState().split(","));
    String host = hostAddress != null ? hostAddress.host : null;
    int port = hostAddress != null ? hostAddress.port : 3306;

    this.socket = ConnectionHelper.createSocket(host, port, conf);
    assignStream(this.socket, conf);

    try {

      final InitialHandshakePacket handshake =
          InitialHandshakePacket.decode(reader.readPacket(true));
      this.exceptionFactory.setThreadId(handshake.getThreadId());
      this.context =
          conf.transactionReplay()
              ? new RedoContext(handshake, conf, this.exceptionFactory, new PrepareCache(100, this))
              : new BaseContext(
                  handshake, conf, this.exceptionFactory, new PrepareCache(100, this));
      this.reader.setServerThreadId(handshake.getThreadId(), hostAddress);
      this.writer.setServerThreadId(handshake.getThreadId(), hostAddress);
      this.writer.setContext(context);

      long clientCapabilities =
          ConnectionHelper.initializeClientCapabilities(conf, this.context.getServerCapabilities());
      byte exchangeCharset = ConnectionHelper.decideLanguage(handshake);

      SSLSocket sslSocket =
          ConnectionHelper.sslWrapper(
              host, socket, clientCapabilities, exchangeCharset, context, writer);

      if (sslSocket != null) {
        assignStream(sslSocket, conf);
        this.reader.setServerThreadId(handshake.getThreadId(), hostAddress);
        this.writer.setServerThreadId(handshake.getThreadId(), hostAddress);
      }

      String authenticationPluginType = handshake.getAuthenticationPluginType();
      CredentialPlugin credentialPlugin = conf.credentialPlugin();
      if (credentialPlugin != null && credentialPlugin.defaultAuthenticationPluginType() != null) {
        authenticationPluginType = credentialPlugin.defaultAuthenticationPluginType();
      }
      Credential credential = ConnectionHelper.loadCredential(credentialPlugin, conf, hostAddress);

      new HandshakeResponse(
              authenticationPluginType,
              context.getSeed(),
              conf,
              host,
              clientCapabilities,
              exchangeCharset)
          .encode(writer, context);
      writer.flush();

      ConnectionHelper.authenticationHandler(credential, writer, reader, context);
      ConnectionHelper.compressionHandler(conf, context);

      if (conf.socketTimeout() > 0) setSocketTimeout(conf.socketTimeout());

      if (!skipPostCommands) {
        postConnectionQueries();
        galeraStateValidation(galeraAllowedStates);
      }

    } catch (IOException ioException) {
      destroySocket();

      String errorMsg =
          String.format(
              "Could not connect to %s:%s : %s", host, socket.getPort(), ioException.getMessage());
      if (host == null) {
        errorMsg = String.format("Could not connect to socket : %s", ioException.getMessage());
      }

      throw exceptionFactory.create(errorMsg, "08000", ioException);
    } catch (SQLException sqlException) {
      destroySocket();
      throw sqlException;
    }
  }

  private void assignStream(Socket socket, Configuration conf) throws SQLException {
    try {
      out = socket.getOutputStream();
      this.writer = new PacketWriter(out, conf.maxQuerySizeToLog(), sequence);
      this.reader = new PacketReader(socket.getInputStream(), conf, sequence);
    } catch (IOException ioe) {
      destroySocket();
      throw exceptionFactory.create("Socket error: " + ioe.getMessage(), "08000", ioe);
    }
  }

  /** Closing socket in case of Connection error after socket creation. */
  private void destroySocket() {
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

  private void galeraStateValidation(List<String> galeraAllowedStates) throws SQLException {
    if (hostAddress != null
        && Boolean.TRUE.equals(hostAddress.primary)
        && !galeraAllowedStates.isEmpty()) {
      List<Completion> res = execute(new QueryPacket("show status like 'wsrep_local_state'"));
      if (res.isEmpty()) {
        throw exceptionFactory.create("fail to validate Galera state");
      }
      ResultSet rs = (ResultSet) res.get(0);
      rs.next();
      if (!galeraAllowedStates.contains(rs.getString(2))) {
        throw exceptionFactory.create(
            String.format("fail to validate Galera state (State is %s)", rs.getString(2)));
      }
    }
  }

  private void postConnectionQueries() throws SQLException {

    try {
      execute(createSessionVariableQuery());
    } catch (SQLException sqlException) {
      // timezone is not valid
      if (conf.timezone() != null) {
        throw exceptionFactory.create(
            String.format(
                "Setting configured timezone '%s' fail on server.\nLook at https://mariadb.com/kb/en/mysql_tzinfo_to_sql/ to load tz data on server, or set timezone=disable to disable setting client timezone.",
                conf.timezone()));
      } else {
        throw exceptionFactory.create(
            String.format(
                "Setting configured client timezone '%s' fail on server.\nLook at https://mariadb.com/kb/en/mysql_tzinfo_to_sql/ to load tz data on server, or set timezone=disable to disable setting client timezone.",
                ZoneId.systemDefault().getId()));
      }
    }

    if (conf.assureReadOnly()
        && !this.hostAddress.primary
        && context.getVersion().versionGreaterOrEqual(5, 6, 5)) {
      execute(new QueryPacket("SET SESSION TRANSACTION READ ONLY"));
    }

    writer.setMaxAllowedPacket(
        conf.maxAllowedPacket() != null
            ? conf.maxAllowedPacket()
            : getMaxAllowedPacketInstance(this));
  }

  public QueryPacket createSessionVariableQuery() {
    // In JDBC, connection must start in autocommit mode
    // [CONJ-269] we cannot rely on serverStatus & ServerStatus.AUTOCOMMIT before this command to
    // avoid this command.
    // if autocommit=0 is set on server configuration, DB always send Autocommit on serverStatus
    // flag
    // after setting autocommit, we can rely on serverStatus value
    StringBuilder sb = new StringBuilder();
    sb.append("autocommit=")
        .append(conf.autocommit() ? "1" : "0")
        .append(", sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')");

    // force schema tracking if available
    if ((context.getServerCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0) {
      sb.append(", session_track_schema=1");
    }

    // add configured session variable if configured
    if (conf.sessionVariables() != null && !conf.sessionVariables().isEmpty()) {
      sb.append(",").append(Security.parseSessionVariables(conf.sessionVariables()));
    }

    // force client timezone to connection to ensure result of now(), ...
    if (conf.timezone() != null && !"disable".equalsIgnoreCase(conf.timezone())) {
      String timeZone =
          (conf.timezone() != null) ? conf.timezone() : ZoneId.systemDefault().getId();
      sb.append(",time_zone='").append(timeZone).append("'");
    }
    return new QueryPacket("set " + sb.toString());
  }

  public static Integer getMaxAllowedPacketInstance(ClientImpl clientImpl) throws SQLException {
    if (MAX_ALLOWED_PACKET == 0) {
      synchronized (MAX_ALLOWED_PACKET) {
        if (MAX_ALLOWED_PACKET == 0) {
          try {
            QueryPacket requestSessionVariables = new QueryPacket("SELECT @@max_allowed_packet");
            List<Completion> completion = clientImpl.execute(requestSessionVariables);
            Result result = (Result) completion.get(0);
            result.next();
            MAX_ALLOWED_PACKET = Integer.parseInt(result.getString(1));
          } catch (SQLException sqlException) {
            // fallback in case of galera non primary nodes that permit only show / set command,
            // not SELECT when not part of quorum
            List<Completion> res =
                clientImpl.execute(
                    new QueryPacket("SHOW VARIABLES WHERE Variable_name = 'max_allowed_packet'"));
            if (res.isEmpty()) {
              throw new SQLException("fail to readAdditionalData");
            }
            ResultSet rs = (ResultSet) res.get(0);
            rs.next();
            if (logger.isDebugEnabled()) {
              logger.debug("server data {} = {}", rs.getString(1), rs.getString(2));
            }
            // max_allowed_packet
            MAX_ALLOWED_PACKET = Integer.parseInt(rs.getString(2));
          }
        }
      }
    }
    return MAX_ALLOWED_PACKET;
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    if (conf.haMode() == HaMode.NONE) {
      execute(
          new QueryPacket("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE")));
    }
  }

  public int sendQuery(ClientMessage message) throws SQLException {
    checkNotClosed();
    try {
      return message.encodePacket(writer, context);
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory
          .withSql(message.description())
          .create("Socket error", "08000", ioException);
    }
  }

  public List<Completion> execute(ClientMessage message) throws SQLException {
    return execute(
        message, null, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  public List<Completion> execute(ClientMessage message, org.mariadb.jdbc.Statement stmt)
      throws SQLException {
    return execute(
        message, stmt, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  public List<Completion> executePipeline(
      ClientMessage[] messages,
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    List<Completion> results = new ArrayList<>();

    int readCounter = 0;
    int[] responseMsg = new int[messages.length];
    try {
      for (int i = 0; i < messages.length; i++) {
        responseMsg[i] = sendQuery(messages[i]);
      }
      for (; readCounter < messages.length; readCounter++) {
        for (int j = 0; j < responseMsg[readCounter]; j++) {
          results.addAll(
              readResponse(
                  stmt,
                  messages[readCounter],
                  fetchSize,
                  maxRows,
                  resultSetConcurrency,
                  resultSetType,
                  closeOnCompletion));
        }
      }
      return results;
    } catch (SQLException sqlException) {

      // read remaining results
      for (int i = ++readCounter; i < messages.length; i++) {
        for (int j = 0; j < responseMsg[readCounter]; j++) {
          try {
            results.addAll(
                readResponse(
                    stmt,
                    messages[readCounter],
                    fetchSize,
                    maxRows,
                    resultSetConcurrency,
                    resultSetType,
                    closeOnCompletion));
          } catch (SQLException e) {
            // eat
          }
        }
      }

      // prepare associated to PrepareStatement need to be uncached
      for (int i = 0; i < results.size(); i++) {
        if (results.get(i) instanceof PrepareResultPacket
            && stmt instanceof ServerPreparedStatement) {
          try {
            ((PrepareResultPacket) results.get(i))
                .decrementUse(this, (ServerPreparedStatement) stmt);
          } catch (SQLException e) {
            // eat
          }
        }
      }

      int batchUpdateLength = 0;
      for (int i = 0; i < messages.length; i++) {
        batchUpdateLength += messages[i].batchUpdateLength();
      }
      throw exceptionFactory.createBatchUpdate(
          results, batchUpdateLength, responseMsg, sqlException);
    }
  }

  public List<Completion> execute(
      ClientMessage message,
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    sendQuery(message);
    return readResponse(
        stmt, message, fetchSize, maxRows, resultSetConcurrency, resultSetType, closeOnCompletion);
  }

  public List<Completion> readResponse(
      org.mariadb.jdbc.Statement stmt,
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

  public List<Completion> readResponse(ClientMessage message) throws SQLException {
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
    return completions;
  }

  public void closePrepare(PrepareResultPacket prepare) throws SQLException {
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

  public void transactionReplay(TransactionSaver transactionSaver) throws SQLException {
    List<RedoableClientMessage> buffers = transactionSaver.getBuffers();
    try {
      // replay all but last
      PrepareResultPacket prepare;
      for (int i = 0; i < buffers.size() - 1; i++) {
        RedoableClientMessage querySaver = buffers.get(i);
        int responseNo;
        if (querySaver instanceof RedoableWithPrepareClientMessage) {
          // command is a prepare statement query
          // redo on new connection need to re-prepare query
          // and substitute statement id
          RedoableWithPrepareClientMessage redoable =
              ((RedoableWithPrepareClientMessage) querySaver);
          String cmd = redoable.getCommand();
          prepare = context.getPrepareCache().get(cmd, redoable.prep());
          if (prepare == null) {
            PreparePacket preparePacket = new PreparePacket(cmd);
            sendQuery(preparePacket);
            prepare = (PrepareResultPacket) readPacket(preparePacket);
          }
          responseNo = querySaver.reExecutePacket(writer, context, prepare);
        } else {
          responseNo = querySaver.reExecutePacket(writer, context, null);
        }
        for (int j = 0; j < responseNo; j++) {
          readResponse(querySaver);
        }
      }
    } catch (IOException e) {
      throw exceptionFactory.create("Socket error during transaction replay", "08000", e);
    }
  }

  private void readResults(ClientMessage message, List<Completion> results) throws SQLException {
    readResults(
        null,
        message,
        results,
        0,
        0L,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.TYPE_FORWARD_ONLY,
        false);
  }

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
      org.mariadb.jdbc.Statement stmt,
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

  public Completion readPacket(ClientMessage message) throws SQLException {
    return readPacket(
        null, message, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  /**
   * Read server response packet.
   *
   * @see <a href="https://mariadb.com/kb/en/mariadb/4-server-response-packets/">server response
   *     packets</a>
   * @param stmt current statement (null if internal)
   * @param message current message
   * @param fetchSize default fetch size
   * @param resultSetConcurrency concurrency
   * @param resultSetType type
   * @param closeOnCompletion must resultset close statement on completion
   * @throws SQLException if any exception
   */
  public Completion readPacket(
      org.mariadb.jdbc.Statement stmt,
      ClientMessage message,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    try {
      Completion completion =
          message.readPacket(
              stmt,
              fetchSize,
              maxRows,
              resultSetConcurrency,
              resultSetType,
              closeOnCompletion,
              reader,
              context,
              exceptionFactory,
              lock);
      if (completion instanceof StreamingResult && !((StreamingResult) completion).loaded()) {
        streamStmt = stmt;
        streamMsg = message;
      }
      return completion;
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory
          .withSql(message.description())
          .create("Socket error", "08000", ioException);
    }
  }

  private void checkNotClosed() throws SQLException {
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

    SQLPermission sqlPermission = new SQLPermission("callAbort");
    SecurityManager securityManager = System.getSecurityManager();
    if (securityManager != null) {
      securityManager.checkPermission(sqlPermission);
    }
    if (executor == null) {
      throw exceptionFactory.create("Cannot abort the connection: null executor passed");
    }

    //    fireConnectionClosed(new ConnectionEvent(this));
    boolean lockStatus = lock.tryLock();

    if (!this.closed) {
      this.closed = true;
      if (!lockStatus) {
        // lock not available : query is running
        // force end by executing an KILL connection
        try (ClientImpl cli = new ClientImpl(conf, hostAddress, false, new ReentrantLock(), true)) {
          cli.execute(new QueryPacket("KILL " + context.getThreadId()));
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
      throw exceptionFactory.create("Cannot set the network timeout", se);
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

  public boolean isPrimary() {
    return hostAddress.primary;
  }

  public ExceptionFactory getExceptionFactory() {
    return exceptionFactory;
  }

  public void reset(ExceptionFactory exceptionFactory) {
    this.exceptionFactory = exceptionFactory;
    this.context.resetPrepareCache(new PrepareCache(250, this));
  }

  public HostAddress getHostAddress() {
    return hostAddress;
  }
}