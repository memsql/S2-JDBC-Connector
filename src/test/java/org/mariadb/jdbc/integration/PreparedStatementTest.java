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

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class PreparedStatementTest extends Common {

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE prepare1");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepare1");
    stmt.execute("CREATE TABLE prepare1 (t1 int not null primary key auto_increment, t2 int)");
  }

  @Test
  public void execute() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      execute(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      execute(con);
    }
  }

  @Test
  public void prep() throws SQLException {
    try (PreparedStatement stmt = sharedConn.prepareStatement("SELECT ?")) {
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }

    try (PreparedStatement stmt =
        sharedConn.prepareStatement(
            "SELECT ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }

    try (PreparedStatement stmt =
        sharedConn.prepareStatement(
            "SELECT ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE,
            ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
      // not supported
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }
  }

  private void execute(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        conn.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      assertFalse(preparedStatement.execute());

      // verification
      ResultSet rs = stmt.executeQuery("SELECT * FROM prepare1");
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertFalse(rs.next());

      // prepare is already done. must only execute.
      preparedStatement.setInt(1, 7);
      preparedStatement.setInt(2, 12);
      assertFalse(preparedStatement.execute());

      rs = stmt.executeQuery("SELECT * FROM prepare1 WHERE t1 > 5");
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertEquals(12, rs.getInt(2));
      assertFalse(rs.next());
    }

    try (PreparedStatement preparedStatement =
        conn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 4);
      assertTrue(preparedStatement.execute());
      ResultSet rs = preparedStatement.getResultSet();
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(7, rs.getInt(1));
      assertEquals(12, rs.getInt(2));
      assertFalse(rs.next());

      preparedStatement.setMaxRows(1);
      preparedStatement.setInt(1, 4);
      assertTrue(preparedStatement.execute());
      rs = preparedStatement.getResultSet();
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      if (isMariaDBServer()) {
        // setMaxRows() has no effect for mysql, since not supporting SET STATEMENT SQL_SELECT_LIMIT
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void executeWithoutAllParameters() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(2, 10);
      assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.executeUpdate(),
          "Parameter at position 1 is not " + "set");

      preparedStatement.setNull(1, Types.VARBINARY);
      preparedStatement.executeUpdate();
      ResultSet rs = stmt.executeQuery("SELECT * FROM prepare1");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void executeUpdate() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      assertEquals(1, preparedStatement.executeUpdate());

      // verification that query without resultset return an empty resultset
      preparedStatement.setInt(2, 11);
      assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.executeQuery(),
          "Parameter at position 1 is not set");
      preparedStatement.setInt(1, 6);
      ResultSet rs0 = preparedStatement.executeQuery();
      assertFalse(rs0.next());

      // verification
      ResultSet rs = stmt.executeQuery("SELECT * FROM prepare1");
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(6, rs.getInt(1));
      assertEquals(11, rs.getInt(2));
      assertFalse(rs.next());
    }

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1")) {
      assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.executeUpdate(),
          "the given SQL statement produces an unexpected ResultSet object");
    }
  }

  @Test
  public void executeQuery() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    stmt.execute("INSERT INTO prepare1(t1, t2) VALUES (5,10), (40,20), (127,45)");
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 20);
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(40, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(127, rs.getInt(1));
      assertEquals(45, rs.getInt(2));
      assertFalse(rs.next());
      assertThrowsContains(
          SQLException.class, () -> preparedStatement.setInt(-20, 2), "wrong parameter index -20");
    }
  }

  @Test
  public void clearParameters() throws Exception {
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      clearParameters(con);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      clearParameters(con);
    }
  }

  public void clearParameters(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE prepare1");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO prepare1(t1, t2) VALUES (?,?)")) {
      preparedStatement.setInt(1, 5);
      preparedStatement.setInt(2, 10);
      preparedStatement.clearParameters();
      assertThrows(SQLException.class, () -> preparedStatement.execute());
    }
  }

  @Test
  public void closeOnCompletion() throws SQLException {
    PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?");
    Assertions.assertFalse(preparedStatement.isCloseOnCompletion());
    preparedStatement.closeOnCompletion();
    Assertions.assertTrue(preparedStatement.isCloseOnCompletion());
    Assertions.assertFalse(preparedStatement.isClosed());
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    Assertions.assertFalse(preparedStatement.isClosed());
    rs.close();
    Assertions.assertTrue(rs.isClosed());
    Assertions.assertTrue(preparedStatement.isClosed());
  }

  @Test
  public void executeBatch() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE prepare1");
    stmt.execute("INSERT INTO prepare1(t1, t2) VALUES (5,10), (40,20), (127,45)");
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement("SELECT * FROM prepare1 WHERE t1 > ?")) {
      preparedStatement.setInt(1, 20);
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(40, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(127, rs.getInt(1));
      assertEquals(45, rs.getInt(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void moreResults() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      moreResults(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      moreResults(con);
    }
  }

  private void moreResults(Connection con) throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = con.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multi");
    stmt.setFetchSize(3);
    stmt.execute(
        "CREATE PROCEDURE multi() BEGIN SELECT * from seq_1_to_10; SELECT * FROM seq_1_to_1000;SELECT 2; END");
    stmt.execute("CALL multi()");
    Assertions.assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(1001, i);
    stmt.setFetchSize(3);
    PreparedStatement prep = con.prepareStatement("CALL multi()");
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults());
    Assertions.assertTrue(rs.isClosed());
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }

    prep.setFetchSize(3);
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
    Assertions.assertFalse(rs.isClosed());
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(11, i);
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(1001, i);

    rs = prep.executeQuery();
    prep.close();
    assertTrue(rs.isClosed());
  }

  @Test
  public void moreRowLimitedResults() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      moreRowLimitedResults(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      moreRowLimitedResults(con);
    }
  }

  private void moreRowLimitedResults(Connection con) throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = con.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multi");
    stmt.setFetchSize(3);
    stmt.setMaxRows(5);
    stmt.execute(
        "CREATE PROCEDURE multi() BEGIN SELECT * from seq_1_to_10; SELECT * FROM seq_1_to_1000;SELECT 2; END");
    stmt.execute("CALL multi()");
    Assertions.assertTrue(stmt.getMoreResults());
    ResultSet rs = stmt.getResultSet();
    int i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(6, i);
    stmt.setFetchSize(3);
    PreparedStatement prep = con.prepareStatement("CALL multi()");
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults());
    Assertions.assertTrue(rs.isClosed());
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }

    prep.setFetchSize(3);
    prep.setMaxRows(5);
    rs = prep.executeQuery();
    Assertions.assertFalse(rs.isClosed());
    prep.setFetchSize(0); // force more result to load all remaining result-set
    Assertions.assertTrue(prep.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT));
    Assertions.assertFalse(rs.isClosed());
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(6, i);
    rs = prep.getResultSet();
    i = 1;
    while (rs.next()) {
      Assertions.assertEquals(i++, rs.getInt(1));
    }
    Assertions.assertEquals(6, i);

    rs = prep.executeQuery();
    prep.close();
    assertTrue(rs.isClosed());
  }

  @Test
  public void prepareWithError() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      prepareWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      prepareWithError(con);
    }
  }

  private void prepareWithError(Connection con) throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepareError");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE prepareError(id int primary key, val varchar(10))");
    stmt.execute("INSERT INTO prepareError(id, val) values (1, 'val1')");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO prepareError(id, val) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "val2");
      assertThrowsContains(
          SQLException.class, () -> prep.execute(), "Duplicate entry '1' for key 'PRIMARY'");
    }
  }

  @Test
  public void expectedError() throws SQLException {
    try (PreparedStatement prep = sharedConn.prepareStatement("SELECT ?")) {
      assertThrowsContains(
          SQLException.class,
          () -> prep.addBatch("SELECT 1"),
          "addBatch(String sql) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1"),
          "execute(String sql) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1", Statement.NO_GENERATED_KEYS),
          "execute(String sql, int autoGeneratedKeys) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1", new int[] {}),
          "execute(String sql, int[] columnIndexes) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.execute("SELECT 1", new String[] {}),
          "execute(String sql, String[] columnNames) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeQuery("SELECT 1"),
          "executeQuery(String sql) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1"),
          "executeUpdate(String sql) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1", Statement.NO_GENERATED_KEYS),
          "executeUpdate(String sql, int autoGeneratedKeys) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1", new int[] {}),
          "executeUpdate(String sql, int[] columnIndexes) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeUpdate("SELECT 1", new String[] {}),
          "executeUpdate(String sql, String[] columnNames) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1"),
          "executeLargeUpdate(String sql) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1", Statement.NO_GENERATED_KEYS),
          "executeLargeUpdate(String sql, int autoGeneratedKeys) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1", new int[] {}),
          "executeLargeUpdate(String sql, int[] columnIndexes) cannot be called on preparedStatement");
      assertThrowsContains(
          SQLException.class,
          () -> prep.executeLargeUpdate("SELECT 1", new String[] {}),
          "executeLargeUpdate(String sql, String[] columnNames) cannot be called on preparedStatement");
    }
  }

  @Test
  public void largeMaxRows() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      largeMaxRows(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      largeMaxRows(con);
    }
  }

  private void largeMaxRows(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS largeMaxRows");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE largeMaxRows(id int)");
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO largeMaxRows(id) VALUE (?)")) {
      for (int i = 1; i < 51; i++) {
        prep.setInt(1, i);
        prep.execute();
      }
    }

    try (PreparedStatement prep = con.prepareStatement("SELECT * FROM largeMaxRows")) {
      assertEquals(0L, prep.getLargeMaxRows());
      ResultSet rs = prep.executeQuery();
      int i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(50, i);

      try {
        prep.setLargeMaxRows(-1);
        Assertions.fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("max rows cannot be negative"));
      }
      prep.setLargeMaxRows(10);
      assertEquals(10L, prep.getLargeMaxRows());

      rs = prep.executeQuery();
      i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(10, i);

      prep.setQueryTimeout(2);
      rs = prep.executeQuery();
      i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(10, i);

      prep.setQueryTimeout(20);
      prep.setLargeMaxRows(0);
      rs = prep.executeQuery();
      i = 0;
      while (rs.next()) {
        i++;
        assertEquals(i, rs.getInt(1));
      }
      assertEquals(50, i);
    }
  }

  @Test
  public void prepareStatementConcur() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      prepareStatementConcur(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      prepareStatementConcur(con);
    }
  }

  private void prepareStatementConcur(Connection con) throws SQLException {
    try (PreparedStatement prep = con.prepareStatement("SELECT 1", new int[] {})) {
      prep.execute();
    }

    try (PreparedStatement prep = con.prepareStatement("SELECT 1", new String[] {})) {
      prep.execute();
    }

    try (PreparedStatement prep =
        con.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS)) {
      assertEquals(ResultSet.CONCUR_READ_ONLY, prep.getResultSetConcurrency());
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, prep.getResultSetType());
      prep.execute();
    }
    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT 1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
      assertEquals(ResultSet.CONCUR_UPDATABLE, prep.getResultSetConcurrency());
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, prep.getResultSetType());

      prep.execute();
    }
  }
}