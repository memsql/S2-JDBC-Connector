// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.integration.tools.TcpProxy;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import org.junit.jupiter.api.Test;

public class MultiHostTest extends Common {

  @Test
  public void closedConnectionMulti() throws Exception {

    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//address=(host=localhost)(port=9999),address=(host=%s)(port=%s)/",
                hostAddress.host, hostAddress.port));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:sequential:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500&useServerPrepStmts&cachePrepStmts=false");
    testClosedConn(con);

    url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//%s:%s,%s,%s/",
                hostAddress.host, hostAddress.port, hostAddress.host, hostAddress.port));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:sequential:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500&useServerPrepStmts&cachePrepStmts=false");
    testClosedConn(con);
  }

  private void testClosedConn(Connection con) throws SQLException {
    PreparedStatement prep = con.prepareStatement("SELECT ?");
    PreparedStatement prep2 = con.prepareStatement("SELECT 1, ?");
    prep2.setString(1, "1");
    prep2.execute();
    Statement stmt = con.createStatement();
    stmt.setFetchSize(1);
    ResultSet rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10");
    rs.next();

    con.close();

    prep.setString(1, "1");
    assertThrowsContains(SQLException.class, () -> prep.execute(), "Connection is closed");
    assertThrowsContains(SQLException.class, () -> prep2.execute(), "Connection is closed");
    assertThrowsContains(SQLException.class, () -> prep2.close(), "Connection is closed");
    con.close();
    assertThrowsContains(
        SQLException.class,
        () -> con.abort(null),
        "Cannot abort the connection: null executor passed");
    assertNotNull(con.getClient().getHostAddress());
    assertThrowsContains(
        SQLException.class,
        () -> con.getClient().readStreamingResults(null, 0, 0, 0, 0, true),
        "Connection is closed");
    con.getClient().reset();
  }

  @Test
  public void streamingFailover() throws Exception {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format("//address=(host=localhost)(port=%s)/", proxy.getLocalPort()));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:sequential:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "allowMultiQueries&transactionReplay=true&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=40&connectTimeout=500&useReadAheadInput=false");
    long threadId = con.getThreadId();
    Statement stmt = con.createStatement();
    stmt.setFetchSize(2);
    ensureRange(stmt);
    ensureLargeRange(stmt, 50000);
    ResultSet rs =
        stmt.executeQuery(
            "SELECT * FROM range_1_100 ORDER BY n; SELECT * FROM large_range ORDER BY n;");
    rs.setFetchSize(0);
    rs.next();
    proxy.restart(50);
    Statement stmt2 = con.createStatement();
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt2.executeQuery("SHOW USERS"),
        "Socket error during result streaming");
    assertNotEquals(threadId, con.getThreadId());

    // additional small test
    assertEquals(0, con.getNetworkTimeout());
    con.setNetworkTimeout(Runnable::run, 10);
    assertEquals(10, con.getNetworkTimeout());

    con.setReadOnly(true);
    con.close();
    Common.assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () -> con.setReadOnly(false),
        "Connection is closed");
    con.abort(Runnable::run); // no-op

    Connection con2 =
        (Connection)
            DriverManager.getConnection(
                url
                    + "allowMultiQueries&transactionReplay=true&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=40&connectTimeout=500&useReadAheadInput=false");
    con2.abort(Runnable::run);
  }
}
