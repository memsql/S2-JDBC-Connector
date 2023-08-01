// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.log.Loggers;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handling connection failing automatic reconnection transparently when possible for replication
 * Topology.
 *
 * <p>remark: would have been better using proxy, but for AOT compilation, avoiding to using not
 * supported proxy class.
 */
public class MultiPrimaryReplicaClient extends MultiPrimaryClient {
  protected long waitTimeout;
  private Client replicaClient;
  private Client primaryClient;
  private boolean requestReadOnly;
  private long nextTryReplica = -1;
  private long nextTryPrimary = -1;

  public MultiPrimaryReplicaClient(Configuration conf, ReentrantLock lock) throws SQLException {
    super(conf, lock);
    primaryClient = currentClient;
    waitTimeout =
        Long.parseLong(conf.nonMappedOptions().getProperty("waitReconnectTimeout", "30000"));
    try {
      replicaClient = connectHost(true, false);
    } catch (SQLException e) {
      replicaClient = null;
      nextTryReplica = System.currentTimeMillis() + waitTimeout;
    }
  }

  private void reconnectIfNeeded() {
    if (!closed) {

      // try reconnect primary
      if (primaryClient == null && nextTryPrimary < System.currentTimeMillis()) {
        try {
          primaryClient = connectHost(false, true);
          nextTryPrimary = -1;
        } catch (SQLException e) {
          nextTryPrimary = System.currentTimeMillis() + waitTimeout;
        }
      }

      // try reconnect replica
      if (replicaClient == null && nextTryReplica < System.currentTimeMillis()) {
        try {
          replicaClient = connectHost(true, true);
          nextTryReplica = -1;
          if (requestReadOnly) {
            syncNewState(primaryClient);
            currentClient = replicaClient;
          }
        } catch (SQLException e) {
          nextTryReplica = System.currentTimeMillis() + waitTimeout;
        }
      }
    }
  }

  /**
   * Reconnect connection, trying to continue transparently if possible. Different cases. * replica
   * fails => reconnect to replica or to master if no replica available
   *
   * <p>if reconnect succeed on replica / use master, no problem, continuing without interruption //
   * if reconnect primary, then replay transaction / throw exception if was in transaction.
   *
   * @throws SQLException if exception
   */
  @Override
  protected void reConnect() throws SQLException {
    denyList.putIfAbsent(
        currentClient.getHostAddress(), System.currentTimeMillis() + deniedListTimeout);
    Loggers.getLogger(MultiPrimaryReplicaClient.class)
        .info("Connection error on {}", currentClient.getHostAddress());
    try {
      Client oldClient = currentClient;
      if (oldClient.isPrimary()) {
        primaryClient = null;
      } else {
        replicaClient = null;
      }

      // remove cached prepare from existing server prepare statement
      oldClient.getContext().resetPrepareCache();

      try {
        currentClient = connectHost(requestReadOnly, requestReadOnly);
        if (requestReadOnly) {
          nextTryReplica = -1;
          replicaClient = currentClient;
        } else {
          nextTryPrimary = -1;
          primaryClient = currentClient;
        }

      } catch (SQLNonTransientConnectionException e) {
        if (requestReadOnly) {
          nextTryReplica = System.currentTimeMillis() + waitTimeout;
          if (primaryClient != null) {
            // connector will use primary client until some replica is up
            currentClient = primaryClient;
          } else {
            // replication fails, and no primary connection
            // trying to create new primary connection
            try {
              primaryClient = connectHost(false, false);
              currentClient = primaryClient;
              nextTryPrimary = -1;
            } catch (SQLNonTransientConnectionException ee) {
              closed = true;
              throw new SQLNonTransientConnectionException(
                  String.format(
                      "Driver has failed to reconnect connection after a "
                          + "communications "
                          + "failure with %s",
                      oldClient.getHostAddress()),
                  "08000");
            }
          }
        } else {
          throw new SQLNonTransientConnectionException(
              String.format(
                  "Driver has failed to reconnect master connection after a "
                      + "communications "
                      + "failure with %s",
                  oldClient.getHostAddress()),
              "08000");
        }
      }

      syncNewState(oldClient);

      // if reconnect succeed on replica / use master, no problem, continuing without interruption
      // if reconnect primary, then replay transaction / throw exception if was in transaction.
      if (!requestReadOnly) {
        if (conf.transactionReplay()) {
          executeTransactionReplay(oldClient);
        } else if ((oldClient.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
          // transaction is lost, but connection is now up again.
          // changing exception to SQLTransientConnectionException
          throw new SQLTransientConnectionException(
              String.format(
                  "Driver has reconnect connection after a "
                      + "communications "
                      + "link "
                      + "failure with %s. In progress transaction was lost",
                  oldClient.getHostAddress()),
              "25S03");
        }
      }

    } catch (SQLNonTransientConnectionException sqle) {
      currentClient = null;
      closed = true;
      if (replicaClient != null) {
        replicaClient.close();
      }
      throw sqle;
    }
  }

  @Override
  public List<Completion> execute(
      ClientMessage message,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    reconnectIfNeeded();
    return super.execute(
        message, stmt, fetchSize, maxRows, resultSetConcurrency, resultSetType, closeOnCompletion);
  }

  @Override
  public List<Completion> executePipeline(
      ClientMessage[] messages,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    reconnectIfNeeded();
    return super.executePipeline(
        messages, stmt, fetchSize, maxRows, resultSetConcurrency, resultSetType, closeOnCompletion);
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
    reconnectIfNeeded();
    super.readStreamingResults(
        completions, fetchSize, maxRows, resultSetConcurrency, resultSetType, closeOnCompletion);
  }

  @Override
  public void closePrepare(Prepare prepare) throws SQLException {
    reconnectIfNeeded();
    super.closePrepare(prepare);
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    reconnectIfNeeded();
    super.abort(executor);
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        if (primaryClient != null) primaryClient.close();
      } catch (SQLException e) {
        // eat
      }
      try {
        if (replicaClient != null) replicaClient.close();
      } catch (SQLException e) {
        // eat
      }
      primaryClient = null;
      replicaClient = null;
    }
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }
    if (readOnly) {
      // changed ?
      if (!requestReadOnly) {
        if (replicaClient != null) {
          currentClient = replicaClient;
          syncNewState(primaryClient);
        } else if (nextTryReplica < System.currentTimeMillis()) {
          try {
            replicaClient = connectHost(true, true);
            currentClient = replicaClient;
            syncNewState(primaryClient);
          } catch (SQLException e) {
            nextTryReplica = System.currentTimeMillis() + waitTimeout;
          }
        }
      }
    } else {
      // changed ?
      if (requestReadOnly) {
        if (primaryClient != null) {
          currentClient = primaryClient;
          syncNewState(replicaClient);
        } else if (nextTryPrimary < System.currentTimeMillis()) {
          try {
            primaryClient = connectHost(false, false);
            nextTryPrimary = -1;
            syncNewState(replicaClient);
          } catch (SQLException e) {
            nextTryPrimary = System.currentTimeMillis() + waitTimeout;
            throw new SQLNonTransientConnectionException(
                "Driver has failed to reconnect a primary connection", "08000");
          }
        }
      }
    }
    requestReadOnly = readOnly;
  }

  @Override
  public int getSocketTimeout() {
    reconnectIfNeeded();
    return super.getSocketTimeout();
  }

  @Override
  public void setSocketTimeout(int milliseconds) throws SQLException {
    reconnectIfNeeded();
    super.setSocketTimeout(milliseconds);
  }

  @Override
  public Context getContext() {
    reconnectIfNeeded();
    return super.getContext();
  }

  @Override
  public ExceptionFactory getExceptionFactory() {
    reconnectIfNeeded();
    return super.getExceptionFactory();
  }

  @Override
  public HostAddress getHostAddress() {
    reconnectIfNeeded();
    return super.getHostAddress();
  }

  public boolean isPrimary() {
    return getHostAddress().primary;
  }

  @Override
  public void reset() {
    if (replicaClient != null) {
      replicaClient.getContext().resetStateFlag();
      replicaClient.getContext().resetPrepareCache();
    }
    if (primaryClient != null) {
      primaryClient.getContext().resetStateFlag();
      primaryClient.getContext().resetPrepareCache();
    }
  }
}
