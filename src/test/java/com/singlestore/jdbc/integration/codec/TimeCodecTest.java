// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Statement;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TimeCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS TimeCodec");
    stmt.execute("DROP TABLE IF EXISTS TimeCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE TimeCodec (t1 TIME(0), t2 TIME(6), t3 TIME(6), t4 TIME, id INT)");
    stmt.execute(
        createRowstore()
            + " TABLE TimeCodec2 (id int not null primary key auto_increment, t1 TIME(0))");
    stmt.execute(
        "INSERT INTO TimeCodec VALUES ('01:55:12', '01:55:13.234567', '-18:30:12.55', null, 1), "
            + "('-838:59:58.999', '838:59:58.999999', '00:00:00', '00:00:00', 2)");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from TimeCodec ORDER BY id");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from TimeCodec"
                + " WHERE 1 > ? ORDER BY id");
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
    return rs;
  }

  @Test
  public void getObject() throws SQLException {
    getObject(get());
  }

  @Test
  public void getObjectPrepare() throws SQLException {
    getObject(getPrepare(sharedConn));
    getObject(getPrepare(sharedConnBinary));
  }

  public void getObject(ResultSet rs) throws SQLException {
    assertEquals(
        6912000, rs.getTime(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("01:55:12").getTime(), rs.getTime(1).getTime());
    assertFalse(rs.wasNull());

    assertEquals(
        6913234, rs.getTime(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("01:55:13").getTime() + 234, rs.getTime(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("-18:-30:-12").getTime() - 550, rs.getTime(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getTime(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(get());
  }

  @Test
  public void getObjectTypePrepare() throws Exception {
    getObjectType(getPrepare(sharedConn));
    getObjectType(getPrepare(sharedConnBinary));
  }

  public void getObjectType(ResultSet rs) throws Exception {
    testErrObject(rs, Integer.class);
    testObject(rs, String.class, "01:55:12");
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testErrObject(rs, Byte.class);
    testErrObject(rs, byte[].class);
    testErrObject(rs, Boolean.class);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
    testErrObject(rs, LocalDate.class);
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("1970-01-01T01:55:12"));
    testObject(rs, LocalTime.class, LocalTime.parse("01:55:12"));
    testObject(rs, Time.class, Time.valueOf("01:55:12"));
    testObject(rs, Timestamp.class, Timestamp.valueOf("1970-01-01 01:55:12"));
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("1970-01-01T01:55:12").atZone(ZoneId.systemDefault()));
    testObject(rs, java.util.Date.class, Timestamp.valueOf("1970-01-01 01:55:12.0"));
    testObject(
        rs,
        Instant.class,
        LocalDateTime.parse("1970-01-01T01:55:12").atZone(ZoneId.systemDefault()).toInstant());
  }

  @Test
  public void getString() throws SQLException {
    getString(get());
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
  }

  public void getString(ResultSet rs) throws SQLException {
    assertEquals("01:55:12", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("01:55:13.234567", rs.getString(2));
    assertEquals("01:55:13.234567", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-18:30:12.550000", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertTrue("-838:59:58".equals(rs.getString(1)) || "-838:59:58.999000".equals(rs.getString(1)));
    assertFalse(rs.wasNull());
    assertEquals("838:59:58.999999", rs.getString(2));
    assertEquals("838:59:58.999999", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("00:00:00.000000", rs.getString(3));
    assertEquals("00:00:00", rs.getString(4));
    assertFalse(rs.wasNull());
  }

  @Test
  public void getNString() throws SQLException {
    getNString(get());
  }

  @Test
  public void getNStringPrepare() throws SQLException {
    getNString(getPrepare(sharedConn));
    getNString(getPrepare(sharedConnBinary));
  }

  public void getNString(ResultSet rs) throws SQLException {
    assertEquals("01:55:12", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("01:55:13.234567", rs.getNString(2));
    assertEquals("01:55:13.234567", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-18:30:12.550000", rs.getNString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBoolean() throws SQLException {
    getBoolean(get());
  }

  @Test
  public void getBooleanPrepare() throws SQLException {
    getBoolean(getPrepare(sharedConn));
    getBoolean(getPrepare(sharedConnBinary));
  }

  public void getBoolean(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getBoolean(1), "Data type TIME cannot be decoded as Boolean");
  }

  @Test
  public void getByte() throws SQLException {
    getByte(get());
  }

  @Test
  public void getBytePrepare() throws SQLException {
    getByte(getPrepare(sharedConn));
    getByte(getPrepare(sharedConnBinary));
  }

  public void getByte(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getByte(1), "Data type TIME cannot be decoded as Byte");
  }

  @Test
  public void getShort() throws SQLException {
    getShort(get());
  }

  @Test
  public void getShortPrepare() throws SQLException {
    getShort(getPrepare(sharedConn));
    getShort(getPrepare(sharedConnBinary));
  }

  public void getShort(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getShort(1), "Data type TIME cannot be decoded as Short");
  }

  @Test
  public void getInt() throws SQLException {
    getInt(get());
  }

  @Test
  public void getIntPrepare() throws SQLException {
    getInt(getPrepare(sharedConn));
    getInt(getPrepare(sharedConnBinary));
  }

  public void getInt(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getInt(1), "Data type TIME cannot be decoded as Integer");
  }

  @Test
  public void getLong() throws SQLException {
    getLong(get());
  }

  @Test
  public void getLongPrepare() throws SQLException {
    getLong(getPrepare(sharedConn));
    getLong(getPrepare(sharedConnBinary));
  }

  public void getLong(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getLong(1), "Data type TIME cannot be decoded as Long");
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(get());
  }

  @Test
  public void getFloatPrepare() throws SQLException {
    getFloat(getPrepare(sharedConn));
    getFloat(getPrepare(sharedConnBinary));
  }

  public void getFloat(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getFloat(1), "Data type TIME cannot be decoded as Float");
  }

  @Test
  public void getDouble() throws SQLException {
    getDouble(get());
  }

  @Test
  public void getDoublePrepare() throws SQLException {
    getDouble(getPrepare(sharedConn));
    getDouble(getPrepare(sharedConnBinary));
  }

  public void getDouble(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getDouble(1), "Data type TIME cannot be decoded as Double");
  }

  @Test
  public void getBigDecimal() throws SQLException {
    getBigDecimal(get());
  }

  @Test
  public void getBigDecimalPrepare() throws SQLException {
    getBigDecimal(getPrepare(sharedConn));
    getBigDecimal(getPrepare(sharedConnBinary));
  }

  public void getBigDecimal(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getBigDecimal(1),
        "Data type TIME cannot be decoded as BigDecimal");
  }

  @Test
  public void getDate() throws SQLException {
    getDate(get());
  }

  @Test
  public void getDatePrepare() throws SQLException {
    getDate(getPrepare(sharedConn));
    getDate(getPrepare(sharedConnBinary));
  }

  public void getDate(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getDate(1), "Data type TIME cannot be decoded as Date");
  }

  @Test
  public void getTime() throws SQLException {
    getTime(get());
  }

  @Test
  public void getTimePrepare() throws SQLException {
    getTime(getPrepare(sharedConn));
    getTime(getPrepare(sharedConnBinary));
  }

  public void getTime(ResultSet rs) throws SQLException {
    assertEquals(
        6912000, rs.getTime(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("01:55:12").getTime(), rs.getTime(1).getTime());
    assertFalse(rs.wasNull());

    assertEquals(
        6913234, rs.getTime(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("01:55:13").getTime() + 234, rs.getTime(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("-18:-30:-12").getTime() - 550, rs.getTime(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getTime(4));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(
        -3020398000L, rs.getTime(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    // Time.valueOf parses -838:59:58 as (starting_point - 838 hours + 59 minutes + 58 seconds),
    // so pass -839:00:02 to get the correct value instead
    Time t = Time.valueOf("-839:00:02");
    assertEquals(Time.valueOf("-839:00:02").getTime(), rs.getTime(1).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        3020398999L, rs.getTime(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("838:59:58").getTime() + 999, rs.getTime(2).getTime());
    assertFalse(rs.wasNull());
    // TODO: PLAT-5956
    assertEquals(Time.valueOf("0:00:00").getTime(), rs.getTime(3).getTime());
    assertFalse(rs.wasNull());
  }

  @Test
  public void getDuration() throws SQLException {
    getDuration(get());
  }

  @Test
  public void getDurationPrepare() throws SQLException {
    getDuration(getPrepare(sharedConn));
    getDuration(getPrepare(sharedConnBinary));
  }

  public void getDuration(ResultSet rs) throws SQLException {
    assertEquals(Duration.parse("PT1H55M12S"), rs.getObject(1, Duration.class));
    assertFalse(rs.wasNull());
    assertEquals(Duration.parse("PT1H55M13.234567S"), rs.getObject(2, Duration.class));
    assertEquals(Duration.parse("PT1H55M13.234567S"), rs.getObject("t2alias", Duration.class));
    assertFalse(rs.wasNull());
    assertEquals(Duration.parse("PT-18H-30M-12.55S"), rs.getObject(3, Duration.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, Duration.class));
    assertTrue(rs.wasNull());

    rs.next();
    assertEquals(Duration.parse("PT-838H-59M-58S"), rs.getObject(1, Duration.class));
    assertFalse(rs.wasNull());
    assertEquals(Duration.parse("PT838H59M58.999999S"), rs.getObject(2, Duration.class));
    assertEquals(Duration.parse("PT838H59M58.999999S"), rs.getObject("t2alias", Duration.class));
    assertFalse(rs.wasNull());
    assertEquals(Duration.parse("PT0S"), rs.getObject(3, Duration.class));
    assertEquals(Duration.parse("PT0S"), rs.getObject(4, Duration.class));
    assertFalse(rs.wasNull());
  }

  @Test
  public void getLocalTime() throws SQLException {
    getLocalTime(get());
  }

  @Test
  public void getLocalTimePrepare() throws SQLException {
    getLocalTime(getPrepare(sharedConn));
    getLocalTime(getPrepare(sharedConnBinary));
  }

  public void getLocalTime(ResultSet rs) throws SQLException {
    assertEquals(LocalTime.parse("01:55:12"), rs.getObject(1, LocalTime.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalTime.parse("01:55:13.234567"), rs.getObject(2, LocalTime.class));
    assertEquals(LocalTime.parse("01:55:13.234567"), rs.getObject("t2alias", LocalTime.class));
    assertFalse(rs.wasNull());
    // Duration.parse("PT-18H-30M-12.55S")
    assertEquals(LocalTime.parse("05:29:47.450"), rs.getObject(3, LocalTime.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, LocalTime.class));
    assertTrue(rs.wasNull());

    rs.next();
    assertEquals(Duration.parse("PT-838H-59M-58S"), rs.getObject(1, Duration.class));
    assertFalse(rs.wasNull());
    assertEquals(Duration.parse("PT838H59M58.999999S"), rs.getObject(2, Duration.class));
    assertEquals(Duration.parse("PT838H59M58.999999S"), rs.getObject("t2alias", Duration.class));
    assertFalse(rs.wasNull());
    assertEquals(Duration.parse("PT0S"), rs.getObject(3, Duration.class));
    assertEquals(Duration.parse("PT0S"), rs.getObject(4, Duration.class));
    assertFalse(rs.wasNull());
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(get());
  }

  @Test
  public void getTimestampPrepare() throws SQLException {
    getTimestamp(getPrepare(sharedConn));
    getTimestamp(getPrepare(sharedConnBinary));
  }

  public void getTimestamp(ResultSet rs) throws SQLException {
    assertEquals(Timestamp.valueOf("1970-01-01 01:55:12").getTime(), rs.getTimestamp(1).getTime());
    assertEquals(
        6912000, rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertEquals(
        Timestamp.valueOf("1970-01-01 01:55:12").getTime(), rs.getTimestamp("t1alias").getTime());
    assertEquals(
        6912000,
        rs.getTimestamp("t1alias", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());

    assertEquals(
        Timestamp.valueOf("1970-01-01 01:55:13.234567").getTime(), rs.getTimestamp(2).getTime());
    assertEquals(
        6913234, rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("1969-12-31 05:29:47.45"), rs.getTimestamp(3));
    assertNull(rs.getTimestamp(4));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("1969-11-27 01:00:02").getTime(), rs.getTimestamp(1).getTime());
    assertEquals(
        -3020398000L,
        rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        3020398999L,
        rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());

    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("1970-02-04 22:59:58.999").getTime(), rs.getTimestamp(2).getTime());

    assertFalse(rs.wasNull());
    // TODO: PLAT-5956
    assertEquals(Timestamp.valueOf("1970-01-01 00:00:00").getTime(), rs.getTime(3).getTime());
    assertFalse(rs.wasNull());
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(get());
  }

  @Test
  public void getAsciiStreamPrepare() throws SQLException {
    getAsciiStream(getPrepare(sharedConn));
    getAsciiStream(getPrepare(sharedConnBinary));
  }

  public void getAsciiStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getAsciiStream(1),
        "Data type TIME cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws Exception {
    getUnicodeStream(get());
  }

  @Test
  public void getUnicodeStreamPrepare() throws Exception {
    getUnicodeStream(getPrepare(sharedConn));
    getUnicodeStream(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getUnicodeStream(1),
        "Data type TIME cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws Exception {
    getBinaryStream(get());
  }

  @Test
  public void getBinaryStreamPrepare() throws Exception {
    getBinaryStream(getPrepare(sharedConn));
    getBinaryStream(getPrepare(sharedConnBinary));
  }

  public void getBinaryStream(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getBinaryStream(1),
        "Data type TIME cannot be decoded as Stream");
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(get());
  }

  @Test
  public void getBytesPrepare() throws SQLException {
    getBytes(getPrepare(sharedConn));
    getBytes(getPrepare(sharedConnBinary));
  }

  public void getBytes(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getBytes(1), "Data type TIME cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws Exception {
    getCharacterStream(get());
  }

  @Test
  public void getCharacterStreamPrepare() throws Exception {
    getCharacterStream(getPrepare(sharedConn));
    getCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getCharacterStream(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getCharacterStream(1),
        "Data type TIME cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws Exception {
    getNCharacterStream(get());
  }

  @Test
  public void getNCharacterStreamPrepare() throws Exception {
    getNCharacterStream(getPrepare(sharedConn));
    getNCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getNCharacterStream(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type TIME cannot be decoded as Reader");
  }

  @Test
  public void getBlob() throws Exception {
    getBlob(get());
  }

  @Test
  public void getBlobPrepare() throws Exception {
    getBlob(getPrepare(sharedConn));
    getBlob(getPrepare(sharedConnBinary));
  }

  public void getBlob(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLException.class, () -> rs.getBlob(1), "Data type TIME cannot be decoded as Blob");
  }

  @Test
  public void getClob() throws Exception {
    getClob(get());
  }

  @Test
  public void getClobPrepare() throws Exception {
    getClob(getPrepare(sharedConn));
    getClob(getPrepare(sharedConnBinary));
  }

  public void getClob(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLException.class, () -> rs.getClob(1), "Data type TIME cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws Exception {
    getNClob(get());
  }

  @Test
  public void getNClobPrepare() throws Exception {
    getNClob(getPrepare(sharedConn));
    getNClob(getPrepare(sharedConnBinary));
  }

  public void getNClob(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLException.class, () -> rs.getNClob(1), "Data type TIME cannot be decoded as Clob");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("TIME", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.sql.Time", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.TIME, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(10, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(10, meta.getColumnDisplaySize(1));
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE TimeCodec2");
    Time tt = Time.valueOf("01:55:12");
    tt.setTime(tt.getTime() + 120000);
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO TimeCodec2(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setTime(2, tt);
      prep.execute();
      prep.setInt(1, 2);
      prep.setTime(2, null);
      prep.execute();
      prep.setInt(1, 3);
      prep.setObject(2, Time.valueOf("01:55:13"));
      prep.execute();
      prep.setInt(1, 4);
      prep.setObject(2, null);
      prep.execute();
      prep.setInt(1, 5);
      prep.setObject(2, Time.valueOf("01:55:14"), Types.TIME);
      prep.execute();
      prep.setInt(1, 6);
      prep.setObject(2, null, Types.TIME);
      prep.execute();
      prep.setInt(1, 7);
      prep.setObject(2, Duration.parse("PT23H54M51S"), Types.TIME);
      prep.execute();
      prep.setInt(1, 8);
      prep.setObject(2, Duration.parse("PT23H54M52S"), Types.TIME);
      prep.execute();
      prep.setInt(1, 9);
      prep.setObject(2, LocalTime.parse("05:29:47"), Types.TIME);
      prep.execute();

      prep.setInt(1, 10);
      prep.setObject(2, LocalTime.parse("05:29:57"), Types.TIME);
      prep.execute();
    }
    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM TimeCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertEquals(tt.getTime(), rs.getTime(2).getTime());
    rs.updateTime("t1", null);
    rs.updateRow();
    assertNull(rs.getTime(2));

    assertTrue(rs.next());
    assertNull(rs.getTime(2));
    rs.updateTime("t1", tt);
    rs.updateRow();
    assertEquals(tt.getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());
    assertEquals(Time.valueOf("01:55:13").getTime(), rs.getTime(2).getTime());
    rs.updateObject(2, Time.valueOf("01:55:14"), JDBCType.TIME);
    rs.updateRow();
    assertEquals(Time.valueOf("01:55:14").getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());
    assertNull(rs.getTime(2));
    rs.updateTime(2, tt);
    rs.updateRow();
    assertEquals(tt.getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());
    assertEquals(Time.valueOf("01:55:14").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertNull(rs.getTime(2));
    assertTrue(rs.next());
    assertEquals(Time.valueOf("23:54:51").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertEquals(Time.valueOf("23:54:52").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertEquals(Time.valueOf("05:29:47").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertEquals(Time.valueOf("05:29:57").getTime(), rs.getTime(2).getTime());

    rs = stmt.executeQuery("SELECT * FROM TimeCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertNull(rs.getTime(2));

    assertTrue(rs.next());
    assertEquals(tt.getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());
    assertEquals(Time.valueOf("01:55:14").getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());
    assertEquals(tt.getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());
    assertEquals(Time.valueOf("01:55:14").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertNull(rs.getTime(2));
    assertTrue(rs.next());
    assertEquals(Time.valueOf("23:54:51").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertEquals(Time.valueOf("23:54:52").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertEquals(Time.valueOf("05:29:47").getTime(), rs.getTime(2).getTime());
    assertTrue(rs.next());
    assertEquals(Time.valueOf("05:29:57").getTime(), rs.getTime(2).getTime());
  }

  @Test
  public void getLocalDateTime() throws SQLException {
    getLocalDateTime(get());
  }

  @Test
  public void getLocalDateTimePrepare() throws SQLException {
    getLocalDateTime(getPrepare(sharedConn));
    getLocalDateTime(getPrepare(sharedConnBinary));
  }

  public void getLocalDateTime(ResultSet rs) throws SQLException {
    assertEquals(LocalDateTime.parse("1970-01-01T01:55:12"), rs.getObject(1, LocalDateTime.class));
    assertFalse(rs.wasNull());
    assertEquals(
        LocalDateTime.parse("1970-01-01T01:55:13.234567"), rs.getObject(2, LocalDateTime.class));
    assertFalse(rs.wasNull());
    assertEquals(
        LocalDateTime.parse("1969-12-31T05:29:47.45"), rs.getObject(3, LocalDateTime.class));
    assertNull(rs.getObject(4, LocalDateTime.class));
    assertTrue(rs.wasNull());
  }
}
