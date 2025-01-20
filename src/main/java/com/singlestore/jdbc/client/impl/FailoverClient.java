// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.context.RedoContext;
import com.singlestore.jdbc.client.util.ClosableLock;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.message.client.ChangeDbPacket;
import com.singlestore.jdbc.message.client.QueryPacket;
import com.singlestore.jdbc.message.client.RedoableWithPrepareClientMessage;
import com.singlestore.jdbc.util.Version;
import com.singlestore.jdbc.util.constants.ConnectionState;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.log.Loggers;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * Handling connection failing automatic reconnection transparently when possible for multi-host
 * Topology.
 *
 * <p>remark: would have been better using proxy, but for AOT compilation, avoiding to using not
 * supported proxy class.
 */
public class FailoverClient implements Client {

  /** temporary blacklisted hosts */
  protected static final ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
  /** denied timeout */
  protected final long deniedListTimeout;
  /** configuration */
  protected final Configuration conf;
  /** is connections explicitly closed */
  protected boolean closed = false;
  /** thread locker */
  protected final ClosableLock lock;
  /** current client */
  protected Client currentClient;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param lock thread locker
   * @throws SQLException if fail to connect
   */
  @SuppressWarnings({"this-escape"})
  public FailoverClient(Configuration conf, ClosableLock lock) throws SQLException {
    this.conf = conf;
    this.lock = lock;
    deniedListTimeout =
        Long.parseLong(conf.nonMappedOptions().getProperty("deniedListTimeout", "60000"));
    currentClient = connectHost();
  }

  /**
   * Trying connecting server.
   *
   * <p>searching each connecting primary / replica connection not temporary denied until found one.
   * searching in temporary denied host if not succeed, until reaching `retriesAllDown` attempts.
   *
   * @return a valid connection client
   * @throws SQLException if not succeed to create a connection.
   */
  protected Client connectHost() throws SQLException {
    int maxRetries = conf.retriesAllDown();

    // First try to connect to available hosts
    try {
      Client client = tryConnectToAvailableHost(maxRetries);
      if (client != null) {
        return client;
      }
    } catch (SQLNonTransientConnectionException | SQLTimeoutException lastException) {
      // Handle fail-fast scenario
    }

    // Verify valid host configuration exists
    validateHostConfiguration();

    // Try connecting to denied hosts as last resort
    return tryConnectToDeniedHost(maxRetries);
  }

  private Client tryConnectToAvailableHost(int retriesLeft) throws SQLException {
    SQLException lastException = null;
    while (retriesLeft > 0) {
      Optional<HostAddress> host = conf.haMode().getAvailableHost(conf.addresses(), denyList);
      if (!host.isPresent()) {
        break;
      }

      try {
        return createClient(host.get());
      } catch (SQLNonTransientConnectionException | SQLTimeoutException e) {
        lastException = e;
        addToDenyList(host.get());
        retriesLeft--;
      }
    }
    if (lastException != null) throw lastException;
    return null;
  }

  private Client tryConnectToDeniedHost(int retriesLeft) throws SQLException {
    SQLNonTransientConnectionException lastException = null;

    while (retriesLeft > 0) {
      Optional<HostAddress> host = findHostWithLowestDenyTimeout();
      if (!host.isPresent()) {
        retriesLeft--;
        continue;
      }

      try {
        Client client = createClient(host.get());
        denyList.remove(host.get());
        return client;
      } catch (SQLNonTransientConnectionException e) {
        lastException = e;
        host.ifPresent(this::addToDenyList);
        retriesLeft--;
        if (retriesLeft > 0) {
          sleepBeforeRetry();
        }
      }
    }

    throw (lastException != null)
        ? lastException
        : new SQLNonTransientConnectionException("No host");
  }

  private Optional<HostAddress> findHostWithLowestDenyTimeout() {
    return denyList.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .filter(e -> conf.addresses().contains(e.getKey()))
        .findFirst()
        .map(Map.Entry::getKey);
  }

  private void validateHostConfiguration() throws SQLNonTransientConnectionException {
    boolean hasValidHost =
        denyList.entrySet().stream().anyMatch(e -> conf.addresses().contains(e.getKey()));

    if (!hasValidHost) {
      throw new SQLNonTransientConnectionException("No host defined");
    }
  }

  private Client createClient(HostAddress host) throws SQLException {
    return conf.transactionReplay()
        ? new ReplayClient(conf, host, lock, false)
        : new StandardClient(conf, host, lock, false);
  }

  private void addToDenyList(HostAddress host) {
    denyList.putIfAbsent(host, System.currentTimeMillis() + deniedListTimeout);
  }

  private void sleepBeforeRetry() {
    try {
      Thread.sleep(250);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Connection loop
   *
   * @return client connection
   * @throws SQLException if fail to connect
   */
  protected Client reConnect() throws SQLException {

    denyList.putIfAbsent(
        currentClient.getHostAddress(), System.currentTimeMillis() + deniedListTimeout);
    Loggers.getLogger(FailoverClient.class)
        .info("Connection error on {}", currentClient.getHostAddress());
    try {
      Client oldClient = currentClient;
      // remove cached prepare from existing server prepare statement
      oldClient.getContext().resetPrepareCache();

      currentClient = connectHost();
      syncNewState(oldClient);
      return oldClient;
    } catch (SQLNonTransientConnectionException sqle) {
      currentClient = null;
      closed = true;
      throw sqle;
    }
  }

  /**
   * Execute transaction replay if in transaction and configured for it, throw an exception if not
   *
   * @param oldClient previous client
   * @param canRedo if command can be redo even if not in transaction
   * @throws SQLException if not able to replay
   */
  protected void replayIfPossible(Client oldClient, boolean canRedo, SQLException cause)
      throws SQLException {
    // oldClient is only valued if this occurs on master.
    if (oldClient != null) {
      if ((oldClient.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        if (conf.transactionReplay()) {
          executeTransactionReplay(oldClient);
        } else {
          // transaction is lost, but connection is now up again.
          // changing exception to SQLTransientConnectionException
          throw new SQLTransientConnectionException(
              String.format(
                  "Driver has reconnect connection after a communications link failure with %s. In progress transaction was lost",
                  oldClient.getHostAddress()),
              "25S03",
              cause);
        }
      } else if (!canRedo) {
        // no transaction, but connection is now up again.
        // changing exception to SQLTransientConnectionException
        throw new SQLTransientConnectionException(
            String.format(
                "Driver has reconnect connection after a communications link failure with %s",
                oldClient.getHostAddress()),
            "25S03",
            cause);
      }
    }
  }

  /**
   * Execute transaction replay
   *
   * @param oldCli previous client
   * @throws SQLException if not able to replay
   */
  protected void executeTransactionReplay(Client oldCli) throws SQLException {
    // transaction replay
    RedoContext ctx = (RedoContext) oldCli.getContext();
    if (ctx.getTransactionSaver().isDirty()) {
      ctx.getTransactionSaver().clear();
      throw new SQLTransientConnectionException(
          String.format(
              "Driver has reconnect connection after a communications link failure with %s. In progress transaction was too big to be replayed, and was lost",
              oldCli.getHostAddress()),
          "25S03");
    }
    ((ReplayClient) currentClient).transactionReplay(ctx.getTransactionSaver());
  }

  /**
   * Synchronized previous and new client states.
   *
   * @param oldCli previous client
   * @throws SQLException if error occurs
   */
  public void syncNewState(Client oldCli) throws SQLException {
    Context oldCtx = oldCli.getContext();
    currentClient.getExceptionFactory().setConnection(oldCli.getExceptionFactory());
    if ((oldCtx.getStateFlag() & ConnectionState.STATE_AUTOCOMMIT) > 0) {
      if ((oldCtx.getServerStatus() & ServerStatus.AUTOCOMMIT)
          != (currentClient.getContext().getServerStatus() & ServerStatus.AUTOCOMMIT)) {
        currentClient.getContext().addStateFlag(ConnectionState.STATE_AUTOCOMMIT);
        currentClient.execute(
            new QueryPacket(
                "set autocommit="
                    + (((oldCtx.getServerStatus() & ServerStatus.AUTOCOMMIT) > 0)
                        ? "true"
                        : "false")),
            true);
      }
    }

    if ((oldCtx.getStateFlag() & ConnectionState.STATE_DATABASE) > 0
        && !Objects.equals(currentClient.getContext().getDatabase(), oldCtx.getDatabase())) {
      currentClient.getContext().addStateFlag(ConnectionState.STATE_DATABASE);
      if (oldCtx.getDatabase() != null) {
        currentClient.execute(new ChangeDbPacket(oldCtx.getDatabase()), true);
      }
      currentClient.getContext().setDatabase(oldCtx.getDatabase());
    }

    if ((oldCtx.getStateFlag() & ConnectionState.STATE_NETWORK_TIMEOUT) > 0) {
      currentClient.setSocketTimeout(oldCli.getSocketTimeout());
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
  public List<Completion> execute(
      ClientMessage message,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {

    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      return currentClient.execute(
          message,
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion,
          canRedo);
    } catch (SQLNonTransientConnectionException e) {
      HostAddress hostAddress = currentClient.getHostAddress();
      Client oldClient = reConnect();

      if (message instanceof QueryPacket && ((QueryPacket) message).isCommit()) {
        throw new SQLTransientConnectionException(
            String.format(
                "Driver has reconnect connection after a "
                    + "communications "
                    + "failure with %s during a COMMIT statement",
                hostAddress),
            "25S03",
            e);
      }

      replayIfPossible(oldClient, canRedo, e);

      if (message instanceof RedoableWithPrepareClientMessage) {
        ((RedoableWithPrepareClientMessage) message).rePrepare(currentClient);
      }
      return currentClient.execute(
          message,
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion,
          canRedo);
    }
  }

  @Override
  public List<Completion> executePipeline(
      ClientMessage[] messages,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      return currentClient.executePipeline(
          messages,
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion,
          canRedo);
    } catch (SQLException e) {
      if (e instanceof SQLNonTransientConnectionException
          || (e.getCause() != null && e.getCause() instanceof SQLNonTransientConnectionException)) {
        Client oldClient = reConnect();
        replayIfPossible(oldClient, canRedo, e);
        Arrays.stream(messages)
            .filter(RedoableWithPrepareClientMessage.class::isInstance)
            .map(RedoableWithPrepareClientMessage.class::cast)
            .forEach(
                rd -> {
                  try {
                    rd.rePrepare(currentClient);
                  } catch (SQLException sqle) {
                    // eat
                  }
                });
        return currentClient.executePipeline(
            messages,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion,
            canRedo);
      }
      throw e;
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
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      currentClient.readStreamingResults(
          completions, fetchSize, maxRows, resultSetConcurrency, resultSetType, closeOnCompletion);
    } catch (SQLNonTransientConnectionException e) {
      try {
        reConnect();
      } catch (SQLException e2) {
        throw getExceptionFactory()
            .create("Socket error during result streaming", e2.getSQLState(), e2);
      }
      throw getExceptionFactory().create("Socket error during result streaming", "HY000", e);
    }
  }

  @Override
  public void closePrepare(Prepare prepare) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      currentClient.closePrepare(prepare);
    } catch (SQLNonTransientConnectionException e) {
      reConnect();
    }
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    currentClient.abort(executor);
  }

  @Override
  public void close() throws SQLException {
    closed = true;
    if (currentClient != null) currentClient.close();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }
  }

  @Override
  public int getSocketTimeout() {
    return currentClient.getSocketTimeout();
  }

  @Override
  public void setSocketTimeout(int milliseconds) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      currentClient.setSocketTimeout(milliseconds);
    } catch (SQLNonTransientConnectionException e) {
      reConnect();
      currentClient.setSocketTimeout(milliseconds);
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public Context getContext() {
    return currentClient.getContext();
  }

  @Override
  public ExceptionFactory getExceptionFactory() {
    return currentClient.getExceptionFactory();
  }

  @Override
  public HostAddress getHostAddress() {
    return currentClient.getHostAddress();
  }

  @Override
  public String getSocketIp() {
    return currentClient.getSocketIp();
  }

  @Override
  public BigInteger getAggregatorId() {
    return currentClient.getAggregatorId();
  }

  @Override
  public BigInteger getInitialSqlSelectLimit() {
    return currentClient.getInitialSqlSelectLimit();
  }

  @Override
  public Version getSingleStoreVersion() {
    return currentClient.getSingleStoreVersion();
  }

  public boolean isPrimary() {
    return true;
  }

  @Override
  public void reset() {
    currentClient.getContext().resetStateFlag();
    currentClient.getContext().resetPrepareCache();
  }
}
