// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc;

import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.impl.StandardClient;
import com.singlestore.jdbc.client.util.ClosableLock;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.message.client.ChangeDbPacket;
import com.singlestore.jdbc.message.client.PingPacket;
import com.singlestore.jdbc.message.client.QueryPacket;
import com.singlestore.jdbc.message.client.ResetPacket;
import com.singlestore.jdbc.plugin.array.FloatArray;
import com.singlestore.jdbc.util.NativeSql;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.constants.ConnectionState;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.timeout.QueryTimeoutHandler;
import com.singlestore.jdbc.util.timeout.QueryTimeoutHandlerImpl;
import java.math.BigInteger;
import java.nio.FloatBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.ConnectionEvent;

public class Connection implements java.sql.Connection {

  private static final Pattern CALLABLE_STATEMENT_PATTERN =
      Pattern.compile(
          "^(\\s*\\{)?\\s*((\\?\\s*=)?(\\s*/\\*([^*]|\\*[^/])*\\*/)*\\s*"
              + "call(\\s*/\\*([^*]|\\*[^/])*\\*/)*\\s*((((`[^`]+`)|([^`\\}]+))\\.)?"
              + "((`[^`]+`)|([^`\\}(]+)))\\s*(\\(.*\\))?(\\s*/\\*([^*]|\\*[^/])*\\*/)*"
              + "\\s*(#.*)?)\\s*(\\}\\s*)?$",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private final ClosableLock lock;
  private final Configuration conf;
  private ExceptionFactory exceptionFactory;
  private final Client client;
  private final Properties clientInfo = new Properties();
  private Boolean tableNameCaseSensitivity;
  private boolean readOnly;
  private final boolean canCachePrepStmts;
  private final int defaultFetchSize;
  private SingleStorePoolConnection poolConnection;
  private final boolean forceTransactionEnd;
  private long sqlSelectLimit;
  private QueryTimeoutHandler queryTimeoutHandler;

  @SuppressWarnings({"this-escape"})
  public Connection(Configuration conf, ClosableLock lock, Client client) {
    this.conf = conf;
    this.forceTransactionEnd =
        Boolean.parseBoolean(conf.nonMappedOptions().getProperty("forceTransactionEnd", "false"));
    this.lock = lock;
    this.exceptionFactory = client.getExceptionFactory().setConnection(this);
    this.client = client;
    Context context = this.client.getContext();
    this.sqlSelectLimit =
        client.getInitialSqlSelectLimit() == null
            ? 0
            : client.getInitialSqlSelectLimit().longValue();
    this.canCachePrepStmts = context.getConf().cachePrepStmts();
    this.defaultFetchSize = context.getConf().defaultFetchSize();
    this.queryTimeoutHandler = new QueryTimeoutHandlerImpl(this, lock);
  }

  public void setPoolConnection(SingleStorePoolConnection poolConnection) {
    this.poolConnection = poolConnection;
    this.exceptionFactory = exceptionFactory.setPoolConnection(poolConnection);
  }

  /**
   * Cancels the current query - clones the current protocol and executes a query using the new
   * connection.
   *
   * @throws SQLException never thrown
   */
  public void cancelCurrentQuery() throws SQLException {
    // prefer relying on IP compare to DNS if not using Unix socket/PIPE
    String currentIp = client.getSocketIp();
    HostAddress hostAddress =
        currentIp == null
            ? client.getHostAddress()
            : HostAddress.from(currentIp, client.getHostAddress().port);
    try (Client cli = new StandardClient(conf, hostAddress, new ClosableLock(), true)) {
      BigInteger aggregatorId = client.getAggregatorId();
      String killQuery =
          String.format("KILL QUERY %d %d", client.getContext().getThreadId(), aggregatorId);
      cli.execute(new QueryPacket(killQuery), false);
    }
  }

  /**
   * Set sql select limit session engine variable.
   *
   * @throws SQLException if a connection error occur
   * @param maxRows limit of rows
   */
  public void setSqlSelectLimit(long maxRows) throws SQLException {
    if (maxRows < 0) {
      throw exceptionFactory.create(
          "sql_select_limit cannot be negative : asked for " + maxRows, "42000");
    }
    String setSelectLimitQuery = String.format("set sql_select_limit=%d", maxRows);
    this.client.execute(new QueryPacket(setSelectLimitQuery), false);
    this.sqlSelectLimit = maxRows;
  }

  /**
   * Get current sql select limit value.
   *
   * @return sql select limit
   */
  public long getSqlSelectLimit() {
    return sqlSelectLimit;
  }

  @Override
  public Statement createStatement() {
    return new Statement(
        this,
        lock,
        Statement.RETURN_GENERATED_KEYS,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        defaultFetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return prepareInternal(
        sql,
        Statement.NO_GENERATED_KEYS,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        conf.useServerPrepStmts());
  }

  /**
   * Prepare statement creation
   *
   * @param sql sql
   * @param autoGeneratedKeys auto generated key required
   * @param resultSetType result-set type
   * @param resultSetConcurrency concurrency
   * @param useBinary use server prepare statement
   * @return prepared statement
   * @throws SQLException if Prepare fails
   */
  public PreparedStatement prepareInternal(
      String sql,
      int autoGeneratedKeys,
      int resultSetType,
      int resultSetConcurrency,
      boolean useBinary)
      throws SQLException {
    checkNotClosed();
    if (useBinary && !sql.startsWith("/*client prepare*/")) {
      try {
        return new ServerPreparedStatement(
            NativeSql.parse(sql, client.getContext()),
            this,
            lock,
            canCachePrepStmts,
            autoGeneratedKeys,
            resultSetType,
            resultSetConcurrency,
            defaultFetchSize);
      } catch (SQLException e) {
        // failover to client
      }
    }
    return new ClientPreparedStatement(
        NativeSql.parse(sql, client.getContext()),
        this,
        lock,
        autoGeneratedKeys,
        resultSetType,
        resultSetConcurrency,
        defaultFetchSize);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return NativeSql.parse(sql, client.getContext());
  }

  @Override
  public boolean getAutoCommit() {
    return (client.getContext().getServerStatus() & ServerStatus.AUTOCOMMIT) > 0;
  }

  @Override
  @SuppressWarnings("try")
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit == getAutoCommit()) {
      return;
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      getContext().addStateFlag(ConnectionState.STATE_AUTOCOMMIT);
      client.execute(new QueryPacket("set autocommit=" + ((autoCommit) ? "true" : "false")), true);
    }
  }

  @Override
  @SuppressWarnings("try")
  public void commit() throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      if ((client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        client.execute(new QueryPacket("COMMIT"), false);
      }
    }
  }

  @Override
  @SuppressWarnings("try")
  public void rollback() throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      if (forceTransactionEnd
          || (client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        client.execute(new QueryPacket("ROLLBACK"), false);
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (poolConnection != null) {
      poolConnection.fireConnectionClosed(new ConnectionEvent(poolConnection));
      return;
    }
    client.close();
  }

  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  /**
   * Connection context.
   *
   * @return connection context.
   */
  public Context getContext() {
    return client.getContext();
  }

  /**
   * Are table case sensitive or not.
   *
   * @return boolean value.
   * @throws SQLException if a connection error occur
   */
  public boolean getTableNameCaseSensitivity() throws SQLException {
    if (tableNameCaseSensitivity == null) {
      if (getMetaData().getSingleStoreVersion().versionGreaterOrEqual(7, 0, 11)) {
        try (java.sql.Statement st = createStatement()) {
          try (ResultSet rs = st.executeQuery("select @@table_name_case_sensitivity")) {
            rs.next();
            tableNameCaseSensitivity = rs.getBoolean(1);
          }
        }
      } else {
        tableNameCaseSensitivity = Boolean.TRUE;
      }
    }
    return tableNameCaseSensitivity;
  }

  @Override
  public DatabaseMetaData getMetaData() {
    return new DatabaseMetaData(this, this.conf);
  }

  @Override
  public boolean isReadOnly() {
    return this.readOnly;
  }

  @Override
  @SuppressWarnings("try")
  public void setReadOnly(boolean readOnly) throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      if (this.readOnly != readOnly) {
        client.setReadOnly(readOnly);
      }
      this.readOnly = readOnly;
      getContext().addStateFlag(ConnectionState.STATE_READ_ONLY);
    }
  }

  @Override
  public String getCatalog() throws SQLException {

    if (client.getContext().hasServerCapability(Capabilities.CLIENT_SESSION_TRACK)) {
      return client.getContext().getDatabase();
    }

    Statement stmt = createStatement();
    ResultSet rs = stmt.executeQuery("select database()");
    rs.next();
    client.getContext().setDatabase(rs.getString(1));
    return client.getContext().getDatabase();
  }

  @Override
  @SuppressWarnings("try")
  public void setCatalog(String catalog) throws SQLException {
    // null catalog means keep current.
    // there is no possibility to set no database when one is selected
    if (catalog == null
        || (client.getContext().hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)
            && catalog.equals(client.getContext().getDatabase()))) {
      return;
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      getContext().addStateFlag(ConnectionState.STATE_DATABASE);
      client.execute(new ChangeDbPacket(catalog), true);
      client.getContext().setDatabase(catalog);
    }
  }

  @Override
  public int getTransactionIsolation() {
    return java.sql.Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  @SuppressWarnings("try")
  public void setTransactionIsolation(int level) throws SQLException {
    String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
    if (level == java.sql.Connection.TRANSACTION_READ_COMMITTED) {
      query += " READ COMMITTED";
    } else {
      throw new SQLException("Unsupported transaction isolation level");
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      checkNotClosed();
      getContext().addStateFlag(ConnectionState.STATE_TRANSACTION_ISOLATION);
      client.getContext().setTransactionIsolationLevel(level);
      client.execute(new QueryPacket(query), true);
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkNotClosed();
    if (client.getContext().getWarning() == 0) {
      return null;
    }

    SQLWarning last = null;
    SQLWarning first = null;

    try (Statement st = this.createStatement()) {
      try (ResultSet rs = st.executeQuery("show warnings")) {
        // returned result set has 'level', 'code' and 'message' columns, in this order.
        while (rs.next()) {
          int code = rs.getInt(2);
          String message = rs.getString(3);
          SQLWarning warning = new SQLWarning(message, null, code);
          if (first == null) {
            first = warning;
          } else {
            last.setNextWarning(warning);
          }
          last = warning;
        }
      }
    }
    return first;
  }

  @Override
  public void clearWarnings() {
    client.getContext().setWarning(0);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkNotClosed();
    return new Statement(
        this,
        lock,
        Statement.RETURN_GENERATED_KEYS,
        resultSetType,
        resultSetConcurrency,
        defaultFetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return prepareInternal(
        sql,
        Statement.RETURN_GENERATED_KEYS,
        resultSetType,
        resultSetConcurrency,
        conf.useServerPrepStmts());
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkNotClosed();
    Matcher matcher = CALLABLE_STATEMENT_PATTERN.matcher(sql);
    if (!matcher.matches()) {
      throw new SQLSyntaxErrorException(
          "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}\n but was : "
              + sql);
    }

    String query = NativeSql.parse(matcher.group(2), client.getContext());

    boolean isFunction = (matcher.group(3) != null);
    String databaseAndProcedure = matcher.group(8);
    String database = matcher.group(10);
    String procedureName = matcher.group(13);
    String arguments = matcher.group(16);
    if (database == null) {
      database = getCatalog();
    }

    if (isFunction) {
      return new FunctionStatement(
          this,
          database,
          databaseAndProcedure,
          (arguments == null) ? "()" : arguments,
          lock,
          canCachePrepStmts,
          resultSetType,
          resultSetConcurrency);
    } else {
      return new ProcedureStatement(
          this,
          query,
          database,
          procedureName,
          lock,
          canCachePrepStmts,
          resultSetType,
          resultSetConcurrency);
    }
  }

  @Override
  public Map<String, Class<?>> getTypeMap() {
    return new HashMap<>();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw exceptionFactory.notSupported("TypeMap are not supported");
  }

  @Override
  public int getHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public void setHoldability(int holdability) {
    // not supported
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(java.sql.Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkNotClosed();
    return new Statement(
        this,
        lock,
        Statement.NO_GENERATED_KEYS,
        resultSetType,
        resultSetConcurrency,
        defaultFetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return prepareInternal(
        sql,
        autoGeneratedKeys,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        conf.useServerPrepStmts());
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
  }

  @Override
  public Clob createClob() {
    return new SingleStoreClob();
  }

  @Override
  public Blob createBlob() {
    return new SingleStoreBlob();
  }

  @Override
  public NClob createNClob() {
    return new SingleStoreClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw exceptionFactory.notSupported("SQLXML type is not supported");
  }

  private void checkNotClosed() throws SQLException {
    if (client.isClosed()) {
      throw exceptionFactory.create("Connection is closed", "08000", 1220);
    }
  }

  @Override
  @SuppressWarnings("try")
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw exceptionFactory.create("the value supplied for timeout is negative");
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      client.execute(PingPacket.INSTANCE, true);
      return true;
    } catch (SQLException sqle) {
      return false;
    }
  }

  @Override
  public void setClientInfo(String name, String value) {
    clientInfo.put(name, value);
  }

  @Override
  public String getClientInfo(String name) {
    return (String) clientInfo.get(name);
  }

  @Override
  public Properties getClientInfo() {
    return clientInfo;
  }

  @Override
  public void setClientInfo(Properties properties) {
    for (Map.Entry<?, ?> entry : properties.entrySet()) {
      clientInfo.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return createArrayOf(typeName, (Object) elements);
  }

  public Array createArrayOf(String typeName, Object elements) throws SQLException {
    if (typeName == null) throw exceptionFactory.notSupported("typeName is not mandatory");
    if (elements == null) return null;

    switch (typeName) {
      case "float":
      case "Float":
        if (float[].class.equals(elements.getClass())) {
          return new FloatArray((float[]) elements, client.getContext());
        }
        if (Float[].class.equals(elements.getClass())) {
          float[] result =
              Arrays.stream(((Float[]) elements))
                  .collect(
                      () -> FloatBuffer.allocate(((Float[]) elements).length),
                      FloatBuffer::put,
                      (left, right) -> {
                        throw new UnsupportedOperationException();
                      })
                  .array();
          return new FloatArray(result, client.getContext());
        }
        throw exceptionFactory.notSupported(
            "elements class is expect to be float[]/Float[] for 'float/Float' typeName");
      default:
        throw exceptionFactory.notSupported(
            String.format("typeName %s is not supported", typeName));
    }
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw exceptionFactory.notSupported("Struct type is not supported");
  }

  @Override
  public String getSchema() {
    // We support only catalog
    return null;
  }

  @Override
  public void setSchema(String schema) {
    // We support only catalog, and JDBC indicate "If the driver does not support schemas, it will
    // silently ignore this request."
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    if (poolConnection != null) {
      SingleStorePoolConnection poolConnection = this.poolConnection;
      poolConnection.close();
      return;
    }
    client.abort(executor);
  }

  @Override
  @SuppressWarnings("try")
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    if (this.isClosed()) {
      throw exceptionFactory.create(
          "Connection.setNetworkTimeout cannot be called on a closed connection");
    }
    if (milliseconds < 0) {
      throw exceptionFactory.create(
          "Connection.setNetworkTimeout cannot be called with a negative timeout");
    }
    getContext().addStateFlag(ConnectionState.STATE_NETWORK_TIMEOUT);

    try (ClosableLock ignore = lock.closeableLock()) {
      client.setSocketTimeout(milliseconds);
    }
  }

  @Override
  public int getNetworkTimeout() {
    return client.getSocketTimeout();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException("The receiver is not a wrapper for " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }

  public Client getClient() {
    return client;
  }

  /**
   * Reset connection set has it was after creating a "fresh" new connection.
   * defaultTransactionIsolation must have been initialized.
   *
   * <p>BUT : - session variable state are reset only if option useResetConnection is set and - if
   * using the option "useServerPrepStmts", PREPARE statement are still prepared
   *
   * @throws SQLException if resetting operation failed
   */
  public void reset() throws SQLException {
    boolean useComReset =
        conf.useResetConnection()
            && getMetaData().getSingleStoreVersion().versionGreaterOrEqual(7, 5, 2);

    if (useComReset) {
      client.execute(ResetPacket.INSTANCE, true);
    }

    // in transaction => rollback
    if (forceTransactionEnd
        || (client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
      client.execute(new QueryPacket("ROLLBACK"), true);
    }

    int stateFlag = getContext().getStateFlag();
    if (stateFlag != 0) {
      try {
        if ((stateFlag & ConnectionState.STATE_NETWORK_TIMEOUT) != 0) {
          setNetworkTimeout(null, conf.socketTimeout());
        }
        if ((stateFlag & ConnectionState.STATE_AUTOCOMMIT) != 0) {
          setAutoCommit(conf.autocommit() == null ? true : conf.autocommit());
        }
        if ((stateFlag & ConnectionState.STATE_DATABASE) != 0) {
          setCatalog(conf.database());
        }
        if ((stateFlag & ConnectionState.STATE_READ_ONLY) != 0) {
          setReadOnly(false); // default to master connection
        }
      } catch (SQLException sqle) {
        throw exceptionFactory.create("error resetting connection");
      }
    }

    client.reset();

    clearWarnings();
  }

  /**
   * Current server thread id.
   *
   * @return current server thread id
   */
  public long getThreadId() {
    return client.getContext().getThreadId();
  }

  /**
   * Fire event to indicate to StatementEventListeners registered on the connection that a
   * PreparedStatement is closed.
   *
   * @param prep prepare statement closing
   */
  public void fireStatementClosed(PreparedStatement prep) {
    if (poolConnection != null) {
      poolConnection.fireStatementClosed(prep);
    }
  }

  protected ExceptionFactory getExceptionFactory() {
    return exceptionFactory;
  }

  /**
   * Return a QueryTimeoutHandler.
   *
   * @param queryTimeout query timeout
   * @return a query timeout handler
   */
  public QueryTimeoutHandler handleTimeout(int queryTimeout) {
    return queryTimeoutHandler.create(queryTimeout);
  }
}
