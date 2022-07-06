// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.result.CompleteResult;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DateTimeCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS DateTimeCodec");
    stmt.execute("DROP TABLE IF EXISTS DateTimeCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE DateTimeCodec (t1 DATETIME , t2 DATETIME(6), t3 DATETIME(6), t4 DATETIME(6), id INT)");
    stmt.execute(
        "INSERT INTO DateTimeCodec VALUES "
            + "('2010-01-12 01:55:12', '1000-01-01 01:55:13.2', '9999-12-31 18:30:12.55', null, 1)");
    stmt.execute(
        createRowstore()
            + " TABLE DateTimeCodec2 (id int not null primary key auto_increment, t1 DATETIME(6))");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateTimeCodec ORDER BY id");
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateTimeCodec"
                + " WHERE 1 > ? ORDER BY id");
    stmt.closeOnCompletion();
    stmt.setInt(1, 0);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
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
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("2010-01-12 01:55:12").getTime(),
        ((Timestamp) rs.getObject(1)).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("1000-01-01 01:55:13.2").getTime(),
        ((Timestamp) rs.getObject(2)).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("9999-12-31 18:30:12.55").getTime(),
        ((Timestamp) rs.getObject(3)).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
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
    testObject(rs, String.class, "2010-01-12 01:55:12");
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
    testObject(rs, LocalDate.class, LocalDate.parse("2010-01-12"));
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("2010-01-12T01:55:12"));
    testObject(rs, LocalTime.class, LocalTime.parse("01:55:12"));
    testObject(rs, Time.class, Time.valueOf("01:55:12"));
    testObject(rs, Timestamp.class, Timestamp.valueOf("2010-01-12 01:55:12"));
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("2010-01-12T01:55:12").atZone(ZoneId.systemDefault()));
    testObject(rs, java.util.Date.class, Date.valueOf("2010-01-12"));
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
    assertEquals("2010-01-12 01:55:12", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1000-01-01 01:55:13.200000", rs.getString(2));
    assertEquals("1000-01-01 01:55:13.200000", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("9999-12-31 18:30:12.550000", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
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
    assertEquals("2010-01-12 01:55:12", rs.getNString(1));
    assertFalse(rs.wasNull());
    String s = rs.getNString(2);
    assertTrue(s.equals("1000-01-01 01:55:13.200000") || s.equals("1000-01-01 01:55:13.200"));
    s = rs.getNString("t2alias");
    assertTrue(s.equals("1000-01-01 01:55:13.200000") || s.equals("1000-01-01 01:55:13.200"));
    assertFalse(rs.wasNull());
    s = rs.getNString(3);
    assertTrue(s.equals("9999-12-31 18:30:12.550000") || s.equals("9999-12-31 18:30:12.550"));
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
        SQLException.class,
        () -> rs.getBoolean(1),
        "Data type DATETIME cannot be decoded as Boolean");
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
        SQLException.class, () -> rs.getByte(1), "Data type DATETIME cannot be decoded as Byte");
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
        SQLException.class, () -> rs.getShort(1), "Data type DATETIME cannot be decoded as Short");
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
        SQLException.class, () -> rs.getInt(1), "Data type DATETIME cannot be decoded as Integer");
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
        SQLException.class, () -> rs.getLong(1), "Data type DATETIME cannot be decoded as Long");
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
        SQLException.class, () -> rs.getFloat(1), "Data type DATETIME cannot be decoded as Float");
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
        SQLException.class,
        () -> rs.getDouble(1),
        "Data type DATETIME cannot be decoded as Double");
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
        "Data type DATETIME cannot be decoded as BigDecimal");
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

  /**
   * This function is to compare Expected Date and Actual Date which are respectively in system's
   * time zone and UTC time zone. It should assert equal both dates regardless of the system's time
   * zone.
   *
   * @param rs
   * @param expectedTime
   * @param index
   * @throws SQLException
   */
  private void testDate(ResultSet rs, long expectedTime, int index) throws SQLException {
    long offset = TimeZone.getDefault().getOffset(expectedTime);
    long time =
        expectedTime % (1000 * 60 * 60 * 24); // milliseconds value of the 'Time' of expectedTime
    long ms_of_24_hrs = 24 * 60 * 60 * 1000; // milliseconds value of 24hrs

    /*
     * While retrieving 'Date' type value from S2 database, it is getting converted into a Date of system's time zone which may result in either
     * previous Date or next Date or same Date as compare to the Date returned from database. This all depends on the time zone difference between database
     * stored Date's time zone and system's time zone.
     *
     * a. If system's offset is in negative and 'Time' value is less than offset value then converted date will be of previous day.
     *    For e.g. date stored in database is as 'x 01:00:00' in UTC format. System offset is -2 so returned date will be converted as 'x-1 23:00:00'.
     *    Hence this returned date needs to be compared against previous date i.e. 'x-1'
     *
     * b. If system's offset is in positive and 'Time' value + offset value is greater than 24 hrs then converted date will be of next day.
     *    For e.g. date stored in database is as 'x 22:00:00' in UTC format. System offset is +4 so returned date will be converted as 'x+1 02:00:00'.
     *    Hence this returned date needs to be compared against next date i.e. 'x+1'
     *
     * c. In all other case, expected and actual date will be of same day.
     *
     */

    if ((offset < 0 && time < Math.abs(offset)) || (offset > 0 && (time + offset) > ms_of_24_hrs)) {
      assertEquals(
          new Date(expectedTime + offset).toString(),
          rs.getDate(index, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
    } else {
      assertEquals(
          new Date(expectedTime).toString(),
          rs.getDate(index, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
    }
  }

  public void getDate(ResultSet rs) throws SQLException {
    testDate(
        rs,
        1263261312000l,
        1); // Passing the millisecond time of the UTC Date (2010-01-12 01:55:12) which is stored in
    // the database at index 1.
    assertFalse(rs.wasNull());

    assertEquals(
        1263254400000L
            - TimeZone.getDefault().getOffset(Timestamp.valueOf("2010-01-12 01:55:12").getTime()),
        rs.getDate(1).getTime());
    assertFalse(rs.wasNull());

    testDate(
        rs,
        -30609785100000l,
        2); // Passing the millisecond time of the UTC Date (1000-01-01 01:55:13.2) which is stored
    // in the database at index 2.
    assertFalse(rs.wasNull());

    assertEquals(
        -30609792000000L
            - TimeZone.getDefault().getOffset(Timestamp.valueOf("1000-01-01 01:55:13").getTime()),
        rs.getDate(2).getTime());

    assertFalse(rs.wasNull());

    assertEquals(
        253402214400000L
            - TimeZone.getDefault().getOffset(Timestamp.valueOf("9999-12-31 18:30:12").getTime()),
        rs.getDate(3).getTime());

    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
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
        6913200, rs.getTime(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("01:55:13").getTime() + 200, rs.getTime(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("18:30:12").getTime() + 550, rs.getTime(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getTime(4));
    assertTrue(rs.wasNull());
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
    assertEquals(Duration.parse("PT265H55M12S"), rs.getObject(1, Duration.class));
    assertEquals(Duration.parse("PT1H55M13.2S"), rs.getObject(2, Duration.class));
    assertNull(rs.getObject(4, Duration.class));
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
    assertEquals(LocalTime.parse("01:55:13.2"), rs.getObject(2, LocalTime.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalTime.parse("18:30:12.55"), rs.getObject(3, LocalTime.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getTime(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLocalDate() throws SQLException {
    getLocalDate(get());
  }

  @Test
  public void getLocalDatePrepare() throws SQLException {
    getLocalDate(getPrepare(sharedConn));
    getLocalDate(getPrepare(sharedConnBinary));
  }

  public void getLocalDate(ResultSet rs) throws SQLException {
    assertEquals(LocalDate.parse("2010-01-12"), rs.getObject(1, LocalDate.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("1000-01-01"), rs.getObject(2, LocalDate.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("9999-12-31"), rs.getObject(3, LocalDate.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, LocalTime.class));
    assertTrue(rs.wasNull());
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
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("2010-01-12 01:55:12").getTime(), rs.getTimestamp(1).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("1000-01-01 01:55:13.2").getTime(), rs.getTimestamp(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("9999-12-31 18:30:12.55").getTime(), rs.getTimestamp(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
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
        "Data type DATETIME cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(get());
  }

  @Test
  public void getUnicodeStreamPrepare() throws SQLException {
    getUnicodeStream(getPrepare(sharedConn));
    getUnicodeStream(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getUnicodeStream(1),
        "Data type DATETIME cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws SQLException {
    getBinaryStream(get());
  }

  @Test
  public void getBinaryStreamPrepare() throws SQLException {
    getBinaryStream(getPrepare(sharedConn));
    getBinaryStream(getPrepare(sharedConnBinary));
  }

  public void getBinaryStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getBinaryStream(1),
        "Data type DATETIME cannot be decoded as Stream");
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
        SQLException.class, () -> rs.getBytes(1), "Data type DATETIME cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(get());
  }

  @Test
  public void getCharacterStreamPrepare() throws SQLException {
    getCharacterStream(getPrepare(sharedConn));
    getCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getCharacterStream(1),
        "Data type DATETIME cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(get());
  }

  @Test
  public void getNCharacterStreamPrepare() throws SQLException {
    getNCharacterStream(getPrepare(sharedConn));
    getNCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getNCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type DATETIME cannot be decoded as Reader");
  }

  @Test
  public void getBlob() throws SQLException {
    getBlob(get());
  }

  @Test
  public void getBlobPrepare() throws SQLException {
    getBlob(getPrepare(sharedConn));
    getBlob(getPrepare(sharedConnBinary));
  }

  public void getBlob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getBlob(1), "Data type DATETIME cannot be decoded as Blob");
  }

  @Test
  public void getBigInteger() throws SQLException {
    getBigInteger(get());
  }

  @Test
  public void getBigIntegerPrepared() throws SQLException {
    getBigInteger(getPrepare(sharedConn));
    getBigInteger(getPrepare(sharedConnBinary));
  }

  private void getBigInteger(ResultSet res) throws SQLException {
    CompleteResult rs = (CompleteResult) res;
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigInteger(1),
        "Data type DATETIME cannot be decoded as BigInteger");
  }

  @Test
  public void getClob() throws SQLException {
    getClob(get());
  }

  @Test
  public void getClobPrepare() throws SQLException {
    getClob(getPrepare(sharedConn));
    getClob(getPrepare(sharedConnBinary));
  }

  public void getClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getClob(1), "Data type DATETIME cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws SQLException {
    getClob(get());
  }

  @Test
  public void getNClobPrepare() throws SQLException {
    getClob(getPrepare(sharedConn));
    getClob(getPrepare(sharedConnBinary));
  }

  public void getNClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getNClob(1), "Data type DATETIME cannot be decoded as Clob");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("DATETIME", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.sql.Timestamp", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.TIMESTAMP, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(19, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(19, meta.getColumnDisplaySize(1));
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE DateTimeCodec2");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO DateTimeCodec2(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setDate(2, Date.valueOf("2010-01-12"));
      prep.execute();
      prep.setInt(1, 2);
      prep.setDate(2, null);
      prep.execute();
      prep.setInt(1, 3);
      prep.setObject(2, Date.valueOf("2010-01-13"));
      prep.execute();
      prep.setInt(1, 4);
      prep.setObject(2, null);
      prep.execute();
      prep.setInt(1, 5);
      prep.setObject(2, Date.valueOf("2010-01-14"), Types.DATE);
      prep.execute();
      prep.setInt(1, 6);
      prep.setObject(2, null, Types.DATE);
      prep.execute();
      prep.setInt(1, 7);
      prep.setObject(2, LocalDateTime.parse("2010-01-12T01:55:12"), Types.TIMESTAMP);
      prep.execute();
      prep.setInt(1, 8);
      prep.setObject(2, LocalDateTime.parse("2010-01-12T01:56:12.456"), Types.TIMESTAMP);
      prep.execute();
      prep.setInt(1, 9);
      prep.setObject(
          2,
          LocalDateTime.parse("2011-01-12T01:55:12").atZone(ZoneId.systemDefault()),
          Types.TIMESTAMP);
      prep.execute();
      prep.setInt(1, 10);
      prep.setObject(
          2,
          LocalDateTime.parse("2011-01-12T01:55:12.456").atZone(ZoneId.systemDefault()),
          Types.TIMESTAMP);
      prep.execute();
      prep.setInt(1, 11);
      prep.setObject(
          2, LocalDateTime.parse("2012-01-12T01:55:12").atZone(ZoneId.of("UTC")), Types.TIMESTAMP);
      prep.execute();
      prep.setInt(1, 12);
      prep.setObject(
          2,
          LocalDateTime.parse("2012-01-12T01:55:12.456").atZone(ZoneId.of("UTC")),
          Types.TIMESTAMP);
      prep.execute();
      prep.setInt(1, 13);
      prep.setTimestamp(2, Timestamp.valueOf("2015-12-12 01:55:12"));
      prep.execute();
      prep.setInt(1, 14);
      prep.setTimestamp(2, Timestamp.valueOf("2015-12-12 01:55:12.654"));
      prep.execute();
      prep.setInt(1, 15);
      prep.setObject(2, Timestamp.valueOf("2016-12-12 01:55:12"));
      prep.execute();
      prep.setInt(1, 16);
      prep.setObject(2, Timestamp.valueOf("2016-12-12 01:55:12.654"));
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM DateTimeCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-12"), rs.getDate(2));
    rs.updateDate(2, null);
    rs.updateRow();
    assertNull(rs.getDate(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateDate(2, Date.valueOf("2011-01-12"));
    rs.updateRow();
    assertEquals(Date.valueOf("2011-01-12"), rs.getDate(2));

    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-13"), rs.getDate(2));
    rs.updateObject(2, null);
    rs.updateRow();
    assertNull(rs.getDate(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject(2, Date.valueOf("2021-01-12"));
    rs.updateRow();
    assertEquals(Date.valueOf("2021-01-12"), rs.getDate(2));

    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-14"), rs.getDate(2));
    rs.updateObject(2, LocalDateTime.parse("2021-01-12T01:55:12"), Types.TIMESTAMP);
    rs.updateRow();
    assertEquals(LocalDateTime.parse("2021-01-12T01:55:12"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateTimestamp(2, Timestamp.valueOf("2015-12-12 01:55:12.654"));
    rs.updateRow();
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(LocalDateTime.parse("2010-01-12T01:55:12"), rs.getObject(2, LocalDateTime.class));
    rs.updateTimestamp("t1", Timestamp.valueOf("2015-12-12 01:55:12.654"));
    rs.updateRow();
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));

    rs = stmt.executeQuery("SELECT * FROM DateTimeCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2011-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2021-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(LocalDateTime.parse("2021-01-12T01:55:12"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));

    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2010-01-12T01:56:12.456"), rs.getObject(2, LocalDateTime.class));

    assertTrue(rs.next());
    assertEquals(LocalDateTime.parse("2011-01-12T01:55:12"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2011-01-12T01:55:12.456"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2012-01-12T01:55:12").atZone(ZoneId.of("UTC")),
        rs.getObject(2, ZonedDateTime.class).withZoneSameInstant(ZoneId.of("UTC")));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2012-01-12T01:55:12.456").atZone(ZoneId.of("UTC")),
        rs.getObject(2, ZonedDateTime.class).withZoneSameInstant(ZoneId.of("UTC")));

    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2016-12-12 01:55:12"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2016-12-12 01:55:12.654"), rs.getTimestamp(2));
  }
}
