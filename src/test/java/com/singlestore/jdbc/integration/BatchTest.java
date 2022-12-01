// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.ClientPreparedStatement;
import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import java.sql.*;
import org.junit.jupiter.api.*;

public class BatchTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BatchTest");
    stmt.execute(
        "CREATE TABLE BatchTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");
  }

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE BatchTest");
  }

  @Test
  public void wrongParameter() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      wrongParameter(con, true);
    }
    try (Connection con = createCon("&useServerPrepStmts=true")) {
      wrongParameter(con, false);
    }
  }

  public void wrongParameter(Connection con, boolean text) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 5);
      try {
        prep.addBatch();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
      }
      try {
        prep.addBatch();
      } catch (SQLException e) {
        assertTrue(
            e.getMessage().contains("Parameter at position 2 is not set")
                || e.getMessage()
                    .contains(
                        "batch set of parameters differ from previous set. All parameters must be set"));
      }

      prep.setInt(1, 5);
      prep.setString(3, "wrong position");
      assertThrowsContains(
          SQLException.class, () -> prep.addBatch(), "Parameter at position 2 is not set");

      prep.setInt(1, 5);
      prep.setString(2, "ok");
      prep.addBatch();
      prep.setString(2, "without position 1");
      assertThrowsContains(
          SQLException.class, () -> prep.addBatch(), "Parameter at " + "position 1 is not set");
    }
  }

  @Test
  public void differentParameterType() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      differentParameterType(con);
    }
    try (Connection con = createCon("&useServerPrepStmts=false&allowLocalInfile")) {
      differentParameterType(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&disablePipeline=true")) {
      differentParameterType(con);
    }
  }

  public void differentParameterType(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "1");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      int[] res = prep.executeBatch();
      assertEquals(2, res.length);
      assertEquals(1, res[0]);
      assertEquals(1, res[1]);
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1, t2");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("2", rs.getString(2));
    assertFalse(rs.next());
  }

  @Test
  public void largeBatch() throws SQLException {
    for (int i = 0; i < 8; i++) {
      boolean useServerPrepStmts = (i & 1) > 0;
      boolean allowLocalInfile = (i & 2) > 0;
      boolean useCompression = (i & 4) > 0;
      boolean rewriteBatchedStatements = !( (i & 1) > 0);

      try (Connection con =
          createCon(
              String.format(
                  "&useServerPrepStmts=%s&allowLocalInfile=%s&useCompression=%s&rewriteBatchedStatements=%s",
                  useServerPrepStmts, allowLocalInfile, useCompression, rewriteBatchedStatements))) {
        largeBatch(con);
      }
    }
  }

  public void largeBatch(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "1");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      prep.executeLargeBatch();
    }
    
    try (PreparedStatement prep =
    		con.prepareStatement("UPDATE BatchTest set t2=? where t1=?")) {
    	prep.setString(1, "11");
    	prep.setInt(2, 1);
    	
    	prep.addBatch();

    	prep.setInt(1, 22);
    	prep.setInt(2, 2);
    	prep.addBatch();
    	prep.executeLargeBatch();
    }
    
    ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest ORDER BY t1, t2");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("11", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("22", rs.getString(2));
    assertFalse(rs.next());
    con.commit();
  }

  @Test
  public void batchWithError() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts=false&allowLocalInfile")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&allowLocalInfile")) {
      batchWithError(con);
    }
  }

  private void batchWithError(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepareError");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE prepareError(id int primary key, val varchar(10))");
    stmt.execute("INSERT INTO prepareError(id, val) values (1, 'val1')");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO prepareError(id, val) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "val3");
      prep.addBatch();
      assertThrowsContains(
          BatchUpdateException.class,
          () -> prep.executeBatch(),
          "Duplicate entry '1' for key 'PRIMARY'");
    }
  }
  
  
  @Test
  public void testInsertRegEx() {
	  String sql = "INSERT INTO BatchTest(t1, t2) VALUES(2,'2')";
	  assertTrue(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());

	  sql = "insert INTO BatchTest(t1, t2) VALUES (?,?)";
	  assertTrue(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());
	  
	  sql = "Insert INTO BatchTest(t1, t2) /*Comment Section*/ VALUES (?,?)";
	  assertTrue(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());
	  
	  sql = "/*Comment Section */ inSERT INTO BatchTest(t1, t2) VALUES (?,?)";
	  assertTrue(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());
	  
	  sql = "inSERT INTO BatchTest(t1, t2) VALUES (?,?) /*Comment Section */ ";
	  assertTrue(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());

	  sql = "Select * from BatchTest";
	  assertFalse(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());
	  
	  sql = "Select * from Insert";
	  assertFalse(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());

	  sql = "delete from BatchTest where t1=1";
	  assertFalse(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());

	  sql = "update BatchTest set t2=21 where t2=1";
	  assertFalse(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());
	  
	  sql = "update Insert set insert='insert' where insert='1'";
	  assertFalse(ClientPreparedStatement.INSERT_STATEMENT_PATTERN.matcher(sql).find());
  }
}
