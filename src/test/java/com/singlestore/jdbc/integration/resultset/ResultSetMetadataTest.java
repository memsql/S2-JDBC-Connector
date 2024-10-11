// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.resultset;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.integration.Common;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ResultSetMetadataTest extends Common {

  private static final String UTF8_CHARSET = "utf8";
  private static final String UTF8MB4_CHARSET = "utf8mb4";
  private static final String BINARY_CHARSET = "binary";
  private static final Class<? extends java.lang.Exception> sqle = SQLException.class;

  @AfterAll
  public static void dropAll() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS ResultSetTest");
    stmt.execute("DROP TABLE IF EXISTS test_rsmd");
    stmt.execute("DROP TABLE IF EXISTS resultsetmetadatatest1");
    stmt.execute("DROP TABLE IF EXISTS resultsetmetadatatest2");
    stmt.execute("DROP TABLE IF EXISTS resultsetmetadatatest3");
    stmt.execute("DROP TABLE IF EXISTS test_rsmd_types");
    stmt.execute("DROP TABLE IF EXISTS test_rsmd_unsigned");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    dropAll();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE ResultSetTest (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("INSERT INTO ResultSetTest(t2) values (1),(2),(3),(4),(5),(6),(7),(8)");
    stmt.execute(
        createRowstore()
            + " TABLE test_rsmd(id_col int not null auto_increment, "
            + "nullable_col varchar(20), unikey_col int , char_col char(10), us smallint unsigned, primary key (id_col, unikey_col))");
    stmt.execute("CREATE TABLE resultsetmetadatatest1(id int, name varchar(20))");
    stmt.execute("CREATE TABLE resultsetmetadatatest2(id int, name varchar(20))");
    stmt.execute("CREATE TABLE resultsetmetadatatest3(id int, name varchar(20))");

    String extendedTypes = "v1 VECTOR(4, F32), v2 VECTOR(2, I64), bs1 BSON";
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS test_rsmd_types (a1 CHAR(7), a2 BINARY(8), a3 VARCHAR(9), "
                + "a4 VARBINARY(10), a5 LONGTEXT, a6 MEDIUMTEXT, a7 TEXT, a8 TINYTEXT, b1 TINYBLOB, b2 BLOB, "
                + "b3 MEDIUMBLOB, b4 LONGBLOB, c JSON, d1 BOOL, d2 BIT, d3 TINYINT, d4 SMALLINT, "
                + "d5 MEDIUMINT, d6 INT, d7 BIGINT, e1 FLOAT, e2 DOUBLE(8, 3), e3 DECIMAL(10, 2), "
                + "f1 DATE, f2 TIME, f3 TIME(6), f4 DATETIME, f5 DATETIME(6), f6 TIMESTAMP, "
                + "f7 TIMESTAMP(6), f8 YEAR%s)",
            minVersion(8, 7, 1) ? ", " + extendedTypes : ""));
    stmt.execute(
        String.format(
            "insert into test_rsmd_types values (null, null, null, null, null, null, "
                + "null, null, null, null, null, null, null, null, null, null, null, "
                + "null, null, null, null, null, null, null, null, null, null, null, "
                + "null, null, null%s)",
            minVersion(8, 7, 1) ? ", null, null, null" : ""));
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS test_rsmd_unsigned (s1 TINYINT, s2 SMALLINT, "
            + "s3 MEDIUMINT, s4 INT, s5 BIGINT, s6 REAL, s7 DOUBLE, s8 DECIMAL, s9 NUMERIC, "
            + "uns1 TINYINT UNSIGNED, uns2 SMALLINT UNSIGNED, uns3 MEDIUMINT UNSIGNED, "
            + "uns4 INT UNSIGNED, uns5 BIGINT UNSIGNED, uns6 REAL UNSIGNED,"
            + " uns7 DOUBLE UNSIGNED, uns8 DECIMAL UNSIGNED, uns9 NUMERIC UNSIGNED)");
    stmt.execute(
        "insert into test_rsmd_unsigned values (null, null, null, null, null, null, "
            + "null, null, null, null, null, null, null, null, null, null, null, null)");
  }

  @Test
  public void metaDataTest() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("insert into test_rsmd (id_col,nullable_col,unikey_col) values (null, 'hej', 9)");
    ResultSet rs =
        stmt.executeQuery(
            "select id_col, nullable_col, unikey_col as something, char_col,us from test_rsmd");
    assertTrue(rs.next());
    ResultSetMetaData rsmd = rs.getMetaData();

    assertTrue(rsmd.isAutoIncrement(1));
    assertFalse(rsmd.isAutoIncrement(2));
    assertEquals(5, rsmd.getColumnCount());
    assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
    assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
    assertEquals(String.class.getName(), rsmd.getColumnClassName(2));
    // auto_increment is BIGINT even as declared as integer
    assertEquals(Long.class.getName(), rsmd.getColumnClassName(1));
    assertEquals(Integer.class.getName(), rsmd.getColumnClassName(3));
    assertEquals("id_col", rsmd.getColumnLabel(1));
    assertEquals("nullable_col", rsmd.getColumnLabel(2));
    assertEquals("something", rsmd.getColumnLabel(3));
    assertEquals("unikey_col", rsmd.getColumnName(3));
    assertEquals("BIGINT", rsmd.getColumnTypeName(1));
    assertEquals("VARCHAR", rsmd.getColumnTypeName(2));
    assertEquals("INT", rsmd.getColumnTypeName(3));
    assertEquals("CHAR", rsmd.getColumnTypeName(4));
    assertEquals("SMALLINT UNSIGNED", rsmd.getColumnTypeName(5));
    assertEquals(Types.BIGINT, rsmd.getColumnType(1));
    assertEquals(Types.VARCHAR, rsmd.getColumnType(2));
    assertEquals(Types.INTEGER, rsmd.getColumnType(3));
    assertEquals(Types.CHAR, rsmd.getColumnType(4));
    assertEquals(Types.INTEGER, rsmd.getColumnType(5));
    assertFalse(rsmd.isReadOnly(1));
    assertFalse(rsmd.isReadOnly(2));
    assertTrue(rsmd.isWritable(1));
    assertTrue(rsmd.isDefinitelyWritable(1));
    assertTrue(rsmd.isCaseSensitive(1));
    assertTrue(rsmd.isSearchable(1));
    assertFalse(rsmd.isCurrency(1));
    assertTrue(rsmd.isSigned(3));
    assertFalse(rsmd.isSigned(5));

    assertThrowsContains(sqle, () -> rsmd.isAutoIncrement(6), "wrong column index 6");
    assertThrowsContains(sqle, () -> rsmd.isReadOnly(6), "wrong column index 6");
    assertThrowsContains(sqle, () -> rsmd.isReadOnly(-6), "wrong column index -6");
    assertThrowsContains(sqle, () -> rsmd.isWritable(6), "wrong column index 6");
    assertThrowsContains(sqle, () -> rsmd.isDefinitelyWritable(6), "wrong column index 6");

    DatabaseMetaData md = sharedConn.getMetaData();
    ResultSet cols = md.getColumns(null, null, "test\\_rsmd", null);
    cols.next();
    assertEquals("id_col", cols.getString("COLUMN_NAME"));
    // auto_increment is BIGINT even as declared as integer
    assertEquals(Types.BIGINT, cols.getInt("DATA_TYPE"));
    cols.next(); /* nullable_col */
    cols.next(); /* unikey_col */
    cols.next(); /* char_col */
    assertEquals("char_col", cols.getString("COLUMN_NAME"));
    assertEquals(Types.CHAR, cols.getInt("DATA_TYPE"));
    cols.next(); /* us */ // CONJ-96: SMALLINT UNSIGNED gives Types.SMALLINT
    assertEquals("us", cols.getString("COLUMN_NAME"));
    assertEquals(Types.SMALLINT, cols.getInt("DATA_TYPE"));

    rs = stmt.executeQuery("select 1 from test_rsmd");
    ResultSetMetaData rsmd2 = rs.getMetaData();
    assertTrue(rsmd2.isReadOnly(1));
    assertFalse(rsmd2.isWritable(1));
    assertFalse(rsmd2.isDefinitelyWritable(1));
  }

  @Test
  public void metaTypesVsColumnTypes() throws SQLException {
    if (minVersion(8, 7, 1)) {
      metaTypesVsColumnTypesWithExtendedTypes(false, false);
      metaTypesVsColumnTypesWithExtendedTypes(false, true);
      metaTypesVsColumnTypesWithExtendedTypes(true, false);
      metaTypesVsColumnTypesWithExtendedTypes(true, true);
    } else {
      metaTypesVsColumnTypes(sharedConn);
    }
  }

  public void metaTypesVsColumnTypesWithExtendedTypes(
      boolean isExtTypeEnabled, boolean isVectorBinary) throws SQLException {
    try (Connection connection =
        createCon(
            String.format(
                "enableExtendedDataTypes=%s&vectorTypeOutputFormat=%s",
                isExtTypeEnabled, isVectorBinary ? "BINARY" : "JSON"))) {
      metaTypesVsColumnTypes(connection);
    }
  }

  public void metaTypesVsColumnTypes(Connection connection) throws SQLException {
    Statement stmt = (Statement) connection.createStatement();
    ResultSet rs = stmt.executeQuery("select * from test_rsmd_types");
    assertTrue(rs.next());
    ResultSetMetaData rsmd = rs.getMetaData();

    DatabaseMetaData md = connection.getMetaData();
    ResultSet cols = md.getColumns(null, null, "test\\_rsmd\\_types", null);
    for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
      cols.next();
      String colName = cols.getString("TYPE_NAME");
      assertEquals(rsmd.getColumnTypeName(i), colName);
      assertEquals(rsmd.getColumnType(i), cols.getInt("DATA_TYPE"));
      // Skip column length comparison for VECTOR type.
      // ColumnDefinitionPacket returns VARCHAR or VARBINARY
      // if extendedTypes = false, vectorOutputFormat=JSON: returns Types.VARCHAR -> precision =
      // Integer.MAX_VALUE / charset
      // if extendedTypes = false, vectorOutputFormat=BINARY: returns Types.VARBINARY -> precision =
      // Integer.MAX_VALUE
      if (i == 32 || i == 33) {
        continue;
      }
      if ("DOUBLE".equals(colName) || "YEAR".equals(colName)) { // PLAT-7210
        continue;
      }
      assertEquals(rsmd.getPrecision(i), cols.getInt("COLUMN_SIZE"));
    }
  }

  @Test
  public void unsignedTypes() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from test_rsmd_unsigned");
    assertTrue(rs.next());
    ResultSetMetaData rsmd = rs.getMetaData();

    for (int i = 1; i <= 9; ++i) {
      assertTrue(rsmd.isSigned(i));
      assertFalse(rsmd.isSigned(i + 9));
    }
  }

  @Test
  public void columnTypesPrecision() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from test_rsmd_types");
    assertTrue(rs.next());
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals(7, rsmd.getPrecision(1));
    assertEquals(8, rsmd.getPrecision(2));
    assertEquals(9, rsmd.getPrecision(3));
    assertEquals(10, rsmd.getPrecision(4));
    assertEquals(Integer.MAX_VALUE, rsmd.getPrecision(5));
    assertEquals(Integer.MAX_VALUE, rsmd.getPrecision(12));
    assertEquals(Integer.MAX_VALUE, rsmd.getPrecision(13));
  }

  @Test
  public void columnTypesDisplaySize() throws SQLException {
    try (com.singlestore.jdbc.Connection connection = createCon()) {
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT @@character_set_server");
        rs.next();
        String defaultCharset = rs.getString(1);
        try {
          columnTypesDisplaySize(UTF8_CHARSET);
          columnTypesDisplaySize(UTF8MB4_CHARSET);
          columnTypesDisplaySize(BINARY_CHARSET);
        } finally {
          stmt.execute(String.format("set global character_set_server='%s'", defaultCharset));
          stmt.execute(String.format("set character_set_server='%s'", defaultCharset));
        }
      }
    }
  }

  private void columnTypesDisplaySize(final String charset) throws SQLException {
    try (com.singlestore.jdbc.Connection connection = createCon()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS test_charset_types");
        stmt.execute(String.format("set global character_set_server='%s'", charset));
        stmt.execute(String.format("set character_set_server='%s'", charset));
        stmt.execute(
            "CREATE ROWSTORE TABLE test_charset_types (a1 CHAR(7), a2 BINARY(8), a3 VARCHAR(9), "
                + "a4 VARBINARY(10), a5 LONGTEXT, a6 MEDIUMTEXT, a7 TEXT, a8 TINYTEXT, b1 TINYBLOB, b2 BLOB, "
                + "b3 MEDIUMBLOB, b4 LONGBLOB, c JSON, d1 BOOL, d2 BIT, d3 TINYINT, d4 SMALLINT, "
                + "d5 MEDIUMINT, d6 INT, d7 BIGINT, e1 FLOAT, e2 DOUBLE(8, 3), e3 DECIMAL(10, 2), "
                + "f1 DATE, f2 TIME, f3 TIME(6), f4 DATETIME, f5 DATETIME(6), f6 TIMESTAMP, "
                + "f7 TIMESTAMP(6), f8 YEAR, o1 ENUM('ON', 'OFF'), o2 SET('v1', 'v2', 'v3', 'v4'), g1 GEOGRAPHY, "
                + "g2 GEOGRAPHYPOINT)");
        stmt.execute(
            "insert into test_charset_types values (null, null, null, null, null, null, "
                + "null, null, null, null, null, null, null, null, null, null, null, "
                + "null, null, null, null, null, null, null, null, null, null, null, "
                + "null, null, null, null, null, null, null)");

        ResultSet rs =
            stmt.executeQuery(
                "select a1 as a1alias, a2 as a2alias, a3 as a3alias, a4 as a4alias, a5 as a5alias, "
                    + "a6 as a6alias, a7 as a7alias, a8 as a8alias, b1 as b1alias, b2 as b2alias, b3 as b3alias, "
                    + "b4 as b4alias, c as calias, d1 as d1alias, d2 as d2alias, d3 as d3alias, d4 as d4alias, "
                    + "d5 as d5alias, d6 as d6alias, d7 as d7alias, e1 as e1alias, e2 as e2alias, "
                    + "e3 as e3alias, f1 as f1alias, f2 as f2alias, f3 as f3alias, f4 as f4alias, "
                    + "f5 as f5alias, f6 as f6alias, f7 as f7alias, f8 as f8alias, o1 as o1alias, o2 as o2alias, "
                    + "g1 as g1alias, g2 as g2alias from test_charset_types");
        assertTrue(rs.next());
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(7, rsmd.getColumnDisplaySize(1));
        assertEquals(8, rsmd.getColumnDisplaySize(2));
        assertEquals(9, rsmd.getColumnDisplaySize(3));
        assertEquals(10, rsmd.getColumnDisplaySize(4));
        // TEXT
        assertEquals(65, 535, rsmd.getColumnDisplaySize(7));
        // BLOB
        assertEquals(65, 535, rsmd.getColumnDisplaySize(10));
        // TINYTEXT
        assertEquals(255, rsmd.getColumnDisplaySize(8));
        // TINYBLOB
        assertEquals(255, rsmd.getColumnDisplaySize(9));
        // MEDIUMTEXT
        assertEquals(16777215, rsmd.getColumnDisplaySize(6));
        // MEDIUMBLOB
        assertEquals(16777215, rsmd.getColumnDisplaySize(11));
        // LONGTEXT
        assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(5));
        // LONGBLOB
        assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(12));
        // JSON
        assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(13));
        // BOOL TINYINT(1) always returns 4
        assertEquals(4, rsmd.getColumnDisplaySize(14));
        // BIT
        assertEquals(1, rsmd.getColumnDisplaySize(15));
        assertEquals(4, rsmd.getColumnDisplaySize(16));
        assertEquals(6, rsmd.getColumnDisplaySize(17));
        assertEquals(9, rsmd.getColumnDisplaySize(18));
        assertEquals(11, rsmd.getColumnDisplaySize(19));
        assertEquals(20, rsmd.getColumnDisplaySize(20));
        assertEquals(12, rsmd.getColumnDisplaySize(21));
        assertEquals(18, rsmd.getColumnDisplaySize(22));
        assertEquals(12, rsmd.getColumnDisplaySize(23));
        assertEquals(10, rsmd.getColumnDisplaySize(24));
        assertEquals(10, rsmd.getColumnDisplaySize(25));
        assertEquals(17, rsmd.getColumnDisplaySize(26));
        assertEquals(19, rsmd.getColumnDisplaySize(27));
        assertEquals(26, rsmd.getColumnDisplaySize(28));
        assertEquals(19, rsmd.getColumnDisplaySize(29));
        assertEquals(26, rsmd.getColumnDisplaySize(30));
        assertEquals(4, rsmd.getColumnDisplaySize(31));
      }
    }
  }

  @Test
  public void conj17() throws Exception {
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery("select count(*),1 from information_schema.tables");
    assertTrue(rs.next());
    assertEquals(rs.getMetaData().getColumnName(1), "count(*)");
    assertEquals(rs.getMetaData().getColumnName(2), "1");
  }

  @Test
  public void conj84() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO resultsetmetadatatest1 VALUES (1, 'foo')");
    stmt.execute("INSERT INTO resultsetmetadatatest2 VALUES (2, 'bar')");
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery(
                "select resultsetmetadatatest1.*, resultsetmetadatatest2.* FROM resultsetmetadatatest1 join resultsetmetadatatest2");
    assertTrue(rs.next());
    assertEquals(rs.findColumn("id"), 1);
    assertEquals(rs.findColumn("name"), 2);
    assertEquals(rs.findColumn("resultsetmetadatatest1.id"), 1);
    assertEquals(rs.findColumn("resultsetmetadatatest1.name"), 2);
    assertEquals(rs.findColumn("resultsetmetadatatest2.id"), 3);
    assertEquals(rs.findColumn("resultsetmetadatatest2.name"), 4);
  }

  @Test
  public void testLongIdentifier() throws Exception {
    try (com.singlestore.jdbc.Connection connection = createCon("&allowMultiQueries=true")) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS TSUPPLY");
        stmt.execute(
            "create table TSUPPLY (RNUM integer not null , SNO varchar(2), PNO varchar(2), JNO varchar(2), QTY integer, SHARD KEY ( RNUM ) )");
        ResultSet rs =
            stmt.executeQuery(
                "SELECT COUNT(`t2`"
                    + new String(new char[1000]).replace("\0", " ")
                    + ") FROM `ResultSetTest`");
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));

        ResultSetMetaData rsmd = rs.getMetaData();

        // database always trims identifier to the first 256 characters
        assertEquals(256, rsmd.getColumnName(1).length());
        assertEquals(
            "COUNT(`t2`" + new String(new char[246]).replace("\0", " "), rsmd.getColumnName(1));
      }
    }
  }

  @Test
  public void testAlias() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testAlias");
    stmt.execute("DROP TABLE IF EXISTS testAlias2");
    stmt.execute("CREATE TABLE testAlias(id int, name varchar(20))");
    stmt.execute("CREATE TABLE testAlias2(id2 int, name2 varchar(20))");

    stmt.execute("INSERT INTO testAlias VALUES (1, 'foo')");
    stmt.execute("INSERT INTO testAlias2 VALUES (2, 'bar')");
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery(
                "select alias1.id as idalias1, "
                    + "alias1.name as namealias1, "
                    + "id2 as idalias2, "
                    + "name2, "
                    + "testAlias.id,"
                    + "alias1.id "
                    + "FROM testAlias as alias1 "
                    + "join testAlias2 as alias2 "
                    + "join testAlias");
    assertTrue(rs.next());

    assertEquals(rs.findColumn("idalias1"), 1);
    assertEquals(rs.findColumn("alias1.idalias1"), 1);

    assertThrowsContains(sqle, () -> rs.findColumn("name"), "Unknown label 'name'");
    assertEquals(rs.findColumn("namealias1"), 2);
    assertEquals(rs.findColumn("alias1.namealias1"), 2);

    assertThrowsContains(sqle, () -> rs.findColumn("id2"), "Unknown label 'id2'");
    assertEquals(rs.findColumn("idalias2"), 3);
    assertEquals(rs.findColumn("alias2.idalias2"), 3);
    assertThrowsContains(
        sqle, () -> rs.findColumn("testAlias2.id2"), "Unknown label 'testAlias2.id2'");

    assertEquals(rs.findColumn("name2"), 4);
    assertThrowsContains(
        sqle, () -> rs.findColumn("testAlias2.name2"), "Unknown label 'testAlias2.name2'");
    assertEquals(rs.findColumn("alias2.name2"), 4);

    assertEquals(rs.findColumn("id"), 5);
    assertEquals(rs.findColumn("testAlias.id"), 5);
    assertEquals(rs.findColumn("alias1.id"), 6);

    assertThrowsContains(
        sqle, () -> rs.findColumn("alias2.name22"), "Unknown label 'alias2.name22'");
    assertThrowsContains(sqle, () -> rs.findColumn(""), "Unknown label ''");
    assertThrowsContains(sqle, () -> rs.findColumn(null), "null is not a valid label value");
  }

  @Test
  public void blankTableNameMeta() throws Exception {
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery(
                "SELECT id AS id_alias FROM resultsetmetadatatest3 AS resultsetmetadatatest1_alias");
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals("resultsetmetadatatest3", rsmd.getTableName(1));
    assertEquals(rsmd.getColumnLabel(1), "id_alias");
    assertEquals(rsmd.getColumnName(1), "id");

    try (Connection connection = createCon("&blankTableNameMeta")) {
      rs =
          connection
              .createStatement()
              .executeQuery(
                  "SELECT id AS id_alias FROM resultsetmetadatatest3 AS resultsetmetadatatest1_alias");
      rsmd = rs.getMetaData();

      assertEquals("", rsmd.getTableName(1));
      assertEquals("id_alias", rsmd.getColumnLabel(1));
      assertEquals("id", rsmd.getColumnName(1));
    }
  }

  @Test
  public void staticMethod() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM resultsetmetadatatest3");
    ResultSetMetaData rsmd = rs.getMetaData();

    rsmd.unwrap(com.singlestore.jdbc.client.result.ResultSetMetaData.class);

    assertThrowsContains(
        SQLException.class,
        () -> rsmd.unwrap(String.class),
        "The receiver is not a wrapper for java.lang.String");
  }

  @Test
  public void databaseResultsetMeta() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();
    ResultSet rs = meta.getTableTypes();
    assertTrue(rs.next());
    ResultSetMetaData rsMeta = rs.getMetaData();
    assertEquals("TABLE_TYPE", rsMeta.getColumnName(1));
    assertEquals("", rsMeta.getTableName(1));
    assertEquals("TABLE", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("TEMPORARY TABLE", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("GLOBAL TEMPORARY TABLE", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("SYSTEM VIEW", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("VIEW", rs.getString(1));
    assertFalse(rs.next());
  }
}
