// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.SingleStoreClob;
import com.singlestore.jdbc.Statement;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class VarcharCodecTest extends CommonCodecTest {
  static String fourByteUnicode = minVersion(7, 5, 0) ? "🌟" : "";

  @AfterAll
  public static void after2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS StringCodec");
    stmt.execute("DROP TABLE IF EXISTS StringParamCodec");
    stmt.execute("DROP TABLE IF EXISTS StringCodecWrong");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    after2();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE StringCodec (t1 VARCHAR(20) CHARACTER SET utf8mb4, t2 VARCHAR(30), t3 VARCHAR(20), t4 VARCHAR(20), id INT) CHARACTER "
            + "SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    stmt.execute(
        "INSERT INTO StringCodec VALUES ('0', '1', 'some"
            + fourByteUnicode
            + "', null, 1), "
            + "('2011-01-01', '2010-12-31 23:59:59.152', '23:54:51.840010', null, 2), "
            + "('aaaa-bb-cc', '0000-00-00 00:00:00', '23:54:51.840010', null, 3),"
            + "('', '', '', null, 4)");
    stmt.execute(
        createRowstore()
            + " TABLE StringParamCodec(id int not null primary key auto_increment, t1 VARCHAR(20)) "
            + "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    stmt.execute(
        "CREATE TABLE StringCodecWrong (t1 VARCHAR(20), id INT) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");

    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from StringCodec ORDER BY id");
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from StringCodec"
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
    assertEquals("0", rs.getObject(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getObject(2));
    assertEquals("1", rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("some" + fourByteUnicode, rs.getObject(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
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
    testObject(rs, Integer.class, Integer.valueOf(0));
    testObject(rs, String.class, "0");
    testObject(rs, Byte.class, Byte.valueOf("0"));
    testObject(rs, Long.class, Long.valueOf(0));
    testObject(rs, Short.class, Short.valueOf((short) 0));
    testObject(rs, BigDecimal.class, BigDecimal.valueOf(0));
    testObject(rs, BigInteger.class, BigInteger.valueOf(0));
    testObject(rs, Double.class, Double.valueOf(0));
    testObject(rs, Float.class, Float.valueOf(0));
    testObject(rs, Byte.class, Byte.valueOf((byte) 0));
    testArrObject(rs, byte[].class, new byte[] {(byte) '0'});
    testObject(rs, Boolean.class, Boolean.FALSE);
    testObject(rs, Clob.class, new SingleStoreClob("0".getBytes()));
    testObject(rs, NClob.class, new SingleStoreClob("0".getBytes()));
    testObject(rs, InputStream.class, new SingleStoreClob("0".getBytes()).getBinaryStream());
    testObject(rs, Reader.class, new StringReader("0"));
    rs.next();
    testObject(rs, LocalDate.class, LocalDate.parse("2011-01-01"));
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("2011-01-01T00:00"));
    testObject(rs, LocalTime.class, LocalTime.parse("23:54:51.840010"), 3);
    Time t = Time.valueOf("23:54:51");
    testObject(rs, Time.class, new Time(t.getTime() + 840), 3);
    testObject(rs, Date.class, Date.valueOf("2011-01-01"));
    Timestamp tt = Timestamp.valueOf("2010-12-31 23:59:59");
    testObject(rs, Timestamp.class, new Timestamp(tt.getTime() + 152), 2);
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("2011-01-01T00:00").atZone(ZoneId.systemDefault()));
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, OffsetDateTime.class);
    testErrObject(rs, OffsetTime.class);
    testObject(rs, java.util.Date.class, Date.valueOf("2010-12-31"), 2);
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
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getString(2));
    assertEquals("1", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("some" + fourByteUnicode, rs.getString(3));
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
    assertEquals("0", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getNString(2));
    assertEquals("1", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("some" + fourByteUnicode, rs.getNString(3));
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
    assertFalse(rs.getBoolean(1));
    assertFalse(rs.wasNull());
    assertTrue(rs.getBoolean(2));
    assertTrue(rs.getBoolean("t2alias"));
    assertFalse(rs.wasNull());
    assertTrue(rs.getBoolean(3));
    assertFalse(rs.wasNull());
    assertFalse(rs.getBoolean(4));
    assertTrue(rs.wasNull());
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
    assertEquals((byte) 0, rs.getByte(1));
    assertFalse(rs.wasNull());
    assertEquals((byte) 1, rs.getByte(2));
    assertEquals((byte) 1, rs.getByte("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getByte(3),
        "value 'some" + fourByteUnicode + "' (VARSTRING) cannot be decoded as Byte");
    assertFalse(rs.wasNull());
    assertEquals((byte) 0, rs.getByte(4));
    assertTrue(rs.wasNull());
    rs.next();
    rs.next();
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getByte(1),
        "value '' (VARSTRING) cannot be decoded as Byte");
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
    assertEquals(0, rs.getShort(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getShort(2));
    assertEquals(1, rs.getShort("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getShort(3),
        "value 'some" + fourByteUnicode + "' cannot be decoded as Short");
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getShort(4));
    assertTrue(rs.wasNull());
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
    assertEquals(0, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getInt(2));
    assertEquals(1, rs.getInt("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getInt(3),
        "value 'some" + fourByteUnicode + "' cannot be decoded as Integer");
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
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
    assertEquals(0L, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(1L, rs.getLong(2));
    assertEquals(1L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getLong(3),
        "value 'some" + fourByteUnicode + "' cannot be decoded as Long");
    assertFalse(rs.wasNull());
    assertEquals(0L, rs.getLong(4));
    assertTrue(rs.wasNull());
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
    assertEquals(0F, rs.getFloat(1));
    assertFalse(rs.wasNull());
    assertEquals(1F, rs.getFloat(2));
    assertEquals(1F, rs.getFloat("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getFloat(3),
        "value 'some" + fourByteUnicode + "' cannot be decoded as Float");
    assertFalse(rs.wasNull());
    assertEquals(0F, rs.getFloat(4));
    assertTrue(rs.wasNull());
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
    assertEquals(0D, rs.getDouble(1));
    assertFalse(rs.wasNull());
    assertEquals(1D, rs.getDouble(2));
    assertEquals(1D, rs.getDouble("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDouble(3),
        "value 'some" + fourByteUnicode + "' cannot be decoded as Double");
    assertFalse(rs.wasNull());
    assertEquals(0D, rs.getDouble(4));
    assertTrue(rs.wasNull());
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
    assertEquals(BigDecimal.ZERO, rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.ONE, rs.getBigDecimal(2));
    assertEquals(BigDecimal.ONE, rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigDecimal(3),
        "value 'some" + fourByteUnicode + "' cannot be decoded as BigDecimal");
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertTrue(rs.wasNull());
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
        SQLDataException.class,
        () -> rs.getDate(1),
        "value '0' (VARSTRING) cannot be decoded as Date");
    rs.next();
    assertEquals("2011-01-01", rs.getDate(1).toString());
    assertFalse(rs.wasNull());
    assertEquals("2010-12-31", rs.getDate(2).toString());
    assertFalse(rs.wasNull());
    assertEquals("2010-12-31", rs.getDate("t2alias").toString());
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate(3),
        "value '23:54:51.840010' (VARSTRING) cannot be decoded as Date");
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate(1),
        "value 'aaaa-bb-cc' (VARSTRING) cannot be decoded as Date");
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
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime(1),
        "VARSTRING value '2011-01-01' cannot be decoded as Time");
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime(2),
        "VARSTRING value '2010-12-31 23:59:59.152' cannot be decoded as Time");
    assertEquals(Time.valueOf("23:54:51").getTime() + 840, rs.getTime(3).getTime());
    assertEquals(Time.valueOf("23:54:51").getTime() + 840, rs.getTime("t3alias").getTime());
    Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    assertEquals(86091840, rs.getTime(3, utc).getTime());
    assertEquals(86091840, rs.getTime("t3alias", utc).getTime());

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
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(1, LocalTime.class),
        "value '2011-01-01' (VARSTRING) cannot be decoded as LocalTime");
    assertFalse(rs.wasNull());

    assertEquals(LocalTime.parse("23:59:59.152"), rs.getObject(2, LocalTime.class));
    assertEquals(LocalTime.parse("23:54:51.840010"), rs.getObject(3, LocalTime.class));
    assertEquals(LocalTime.parse("23:54:51.840010"), rs.getObject("t3alias", LocalTime.class));
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(1, Duration.class),
        "VARSTRING value '0' cannot be decoded as Time");
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(1, Duration.class),
        "VARSTRING value '2011-01-01' cannot be decoded as Time");
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(2, Duration.class),
        "VARSTRING value '2010-12-31 23:59:59.152' cannot be decoded as Time");
    assertEquals(Duration.parse("PT23H54M51.84001S"), rs.getObject(3, Duration.class));
    assertEquals(Duration.parse("PT23H54M51.84001S"), rs.getObject("t3alias", Duration.class));
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
    int offset = getOffsetAtDate(2011, 1, 1);
    rs.next();

    assertEquals(Timestamp.valueOf("2011-01-01 00:00:00").getTime(), rs.getTimestamp(1).getTime());
    assertEquals(
        Timestamp.valueOf("2011-01-01 00:00:00").getTime() + offset,
        rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertEquals(
        Timestamp.valueOf("2011-01-01 00:00:00").getTime(), rs.getTimestamp("t1alias").getTime());
    assertEquals(
        Timestamp.valueOf("2011-01-01 00:00:00").getTime() + offset,
        rs.getTimestamp("t1alias", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());

    assertEquals(
        Timestamp.valueOf("2010-12-31 23:59:59").getTime() + 152, rs.getTimestamp(2).getTime());
    assertEquals(
        Timestamp.valueOf("2010-12-31 23:59:59").getTime() + 152 + offset,
        rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertEquals(
        Timestamp.valueOf("2010-12-31 23:59:59").getTime() + 152,
        rs.getTimestamp("t2alias").getTime());
    assertEquals(
        Timestamp.valueOf("2010-12-31 23:59:59").getTime() + 152 + offset,
        rs.getTimestamp("t2alias", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(1, LocalDateTime.class),
        "value '0' (VARSTRING) cannot be decoded as LocalDateTime");
    rs.next();
    assertEquals(LocalDateTime.parse("2011-01-01T00:00:00"), rs.getObject(1, LocalDateTime.class));
    assertFalse(rs.wasNull());
    assertEquals(
        LocalDateTime.parse("2010-12-31T23:59:59.152"), rs.getObject(2, LocalDateTime.class));
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(1, LocalDateTime.class),
        "value 'aaaa-bb-cc' (VARSTRING) cannot be decoded as LocalDateTime");
    assertNull(rs.getObject(2, LocalDateTime.class));
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(1, LocalDate.class),
        "value '0' (VARSTRING) cannot be decoded as Date");
    rs.next();
    assertEquals(LocalDate.parse("2011-01-01"), rs.getObject(1, LocalDate.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("2010-12-31"), rs.getObject(2, LocalDate.class));
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getObject(1, LocalDate.class),
        "value 'aaaa-bb-cc' (VARSTRING) cannot be decoded as Date");
    assertNull(rs.getObject(2, LocalDate.class));
  }

  @Test
  public void getAsciiStream() throws Exception {
    getAsciiStream(get());
  }

  @Test
  public void getAsciiStreamPrepare() throws Exception {
    getAsciiStream(getPrepare(sharedConn));
    getAsciiStream(getPrepare(sharedConnBinary));
  }

  public void getAsciiStream(ResultSet rs) throws Exception {
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getAsciiStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getAsciiStream(2));
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getAsciiStream("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream(("some" + fourByteUnicode).getBytes(StandardCharsets.UTF_8)),
        rs.getAsciiStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getAsciiStream(4));
    assertTrue(rs.wasNull());
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
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getUnicodeStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getUnicodeStream(2));
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getUnicodeStream("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream(("some" + fourByteUnicode).getBytes(StandardCharsets.UTF_8)),
        rs.getUnicodeStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getUnicodeStream(4));
    assertTrue(rs.wasNull());
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
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getBinaryStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getBinaryStream(2));
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getBinaryStream("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream(("some" + fourByteUnicode).getBytes(StandardCharsets.UTF_8)),
        rs.getBinaryStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBinaryStream(4));
    assertTrue(rs.wasNull());
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
    assertArrayEquals("0".getBytes(), rs.getBytes(1));
    assertFalse(rs.wasNull());
    assertArrayEquals("1".getBytes(), rs.getBytes(2));
    assertArrayEquals("1".getBytes(), rs.getBytes("t2alias"));
    assertFalse(rs.wasNull());
    assertArrayEquals(("some" + fourByteUnicode).getBytes(StandardCharsets.UTF_8), rs.getBytes(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBytes(4));
    assertTrue(rs.wasNull());
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
    assertReaderEquals(new StringReader("0"), rs.getCharacterStream(1));
    assertFalse(rs.wasNull());
    assertReaderEquals(new StringReader("1"), rs.getCharacterStream(2));
    assertReaderEquals(new StringReader("1"), rs.getCharacterStream("t2alias"));
    assertFalse(rs.wasNull());
    assertReaderEquals(new StringReader("some" + fourByteUnicode), rs.getCharacterStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getCharacterStream(4));
    assertTrue(rs.wasNull());
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
    assertReaderEquals(new StringReader("0"), rs.getNCharacterStream(1));
    assertFalse(rs.wasNull());
    assertReaderEquals(new StringReader("1"), rs.getNCharacterStream(2));
    assertReaderEquals(new StringReader("1"), rs.getNCharacterStream("t2alias"));
    assertFalse(rs.wasNull());
    assertReaderEquals(new StringReader("some" + fourByteUnicode), rs.getNCharacterStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNCharacterStream(4));
    assertTrue(rs.wasNull());
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
        SQLDataException.class,
        () -> rs.getBlob(1),
        "Data type VARSTRING (not binary) cannot be decoded as Blob");
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
    assertStreamEquals(new SingleStoreClob("0".getBytes()), rs.getClob(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new SingleStoreClob("1".getBytes()), rs.getClob(2));
    assertStreamEquals(new SingleStoreClob("1".getBytes()), rs.getClob("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new SingleStoreClob(("some" + fourByteUnicode).getBytes(StandardCharsets.UTF_8)),
        rs.getClob(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getClob(4));
    assertTrue(rs.wasNull());
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
    assertStreamEquals(new SingleStoreClob("0".getBytes()), rs.getNClob(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new SingleStoreClob("1".getBytes()), rs.getNClob(2));
    assertStreamEquals(new SingleStoreClob("1".getBytes()), rs.getNClob("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new SingleStoreClob(("some" + fourByteUnicode).getBytes(StandardCharsets.UTF_8)),
        rs.getNClob(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNClob(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("VARSTRING", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.String", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.VARCHAR, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    // TODO: PLAT-5895
    // assertEquals(20, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    // TODO: PLAT-5895
    // assertEquals(20, meta.getColumnDisplaySize(1));
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
    try (Connection con = createCon()) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      sendParam(con);
    }
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE StringParamCodec");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO StringParamCodec(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "e'\\n" + fourByteUnicode + "'\\'1Ã");
      prep.execute();
      prep.setInt(1, 2);
      prep.setString(2, null);
      prep.execute();
      prep.setInt(1, 3);
      prep.setObject(2, "e" + fourByteUnicode + "2");
      prep.execute();
      prep.setInt(1, 4);
      prep.setObject(2, null);
      prep.execute();
      prep.setInt(1, 5);
      prep.setObject(2, "e" + fourByteUnicode + "3", Types.VARCHAR);
      prep.execute();
      prep.setInt(1, 6);
      prep.setObject(2, null, Types.VARCHAR);
      prep.execute();
      prep.setInt(1, 7);
      prep.setAsciiStream(
          2,
          new ByteArrayInputStream(
              ("e" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setInt(1, 8);
      prep.setAsciiStream(
          2,
          new ByteArrayInputStream(
              ("e" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)),
          3 + 2 * fourByteUnicode.length());
      prep.execute();
      prep.setInt(1, 9);
      prep.setAsciiStream(
          2,
          new ByteArrayInputStream(
              ("e" + fourByteUnicode + "4654").getBytes(StandardCharsets.UTF_8)),
          3L + 2 * fourByteUnicode.length());
      prep.execute();
      prep.setInt(1, 10);
      prep.setAsciiStream(
          2,
          new ByteArrayInputStream(
              ("e" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setInt(1, 11);
      prep.setAsciiStream(
          2,
          new ByteArrayInputStream(
              ("e" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)),
          3 + 2 * fourByteUnicode.length());
      prep.execute();
      prep.setInt(1, 12);
      prep.setAsciiStream(
          2,
          new ByteArrayInputStream(
              ("e" + fourByteUnicode + "4654").getBytes(StandardCharsets.UTF_8)),
          3L + 2 * fourByteUnicode.length());
      prep.execute();
      prep.setInt(1, 13);
      prep.setNString(2, "e" + fourByteUnicode + "13");
      prep.execute();
      prep.setInt(1, 14);
      prep.setNString(2, "e" + fourByteUnicode + "12");
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM StringParamCodec ORDER BY id");
    assertTrue(rs.next());
    assertEquals("e'\\n" + fourByteUnicode + "'\\'1Ã", rs.getString(2));
    rs.updateString("t1", "f" + fourByteUnicode + "12");
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "12", rs.getString(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertNull(rs.getURL(2));
    rs.updateString(2, "f" + fourByteUnicode + "125");
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "125", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "2", rs.getString(2));
    rs.updateString(2, null);
    rs.updateRow();
    assertNull(rs.getString(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject(
        2, "f" + fourByteUnicode + "125", JDBCType.VARCHAR, 3 + fourByteUnicode.length());
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "12", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "3", rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "3654", rs.getString(2));
    rs.updateAsciiStream(
        "t1",
        new ByteArrayInputStream(
            ("f" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)));
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "3654", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "36", rs.getString(2));
    rs.updateAsciiStream(
        "t1",
        new ByteArrayInputStream(("f" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)),
        3 + 2 * fourByteUnicode.length());
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "36", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "46", rs.getString(2));
    rs.updateAsciiStream(
        "t1",
        new ByteArrayInputStream(("f" + fourByteUnicode + "4654").getBytes(StandardCharsets.UTF_8)),
        3L + 2 * fourByteUnicode.length());
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "46", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "3654", rs.getString(2));
    rs.updateAsciiStream(
        2,
        new ByteArrayInputStream(
            ("f" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)));
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "3654", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "36", rs.getString(2));
    rs.updateAsciiStream(
        2,
        new ByteArrayInputStream(("f" + fourByteUnicode + "3654").getBytes(StandardCharsets.UTF_8)),
        3 + 2 * fourByteUnicode.length());
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "36", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "46", rs.getString(2));
    rs.updateAsciiStream(
        2,
        new ByteArrayInputStream(("f" + fourByteUnicode + "4654").getBytes(StandardCharsets.UTF_8)),
        3L + 2 * fourByteUnicode.length());
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "46", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "13", rs.getString(2));
    rs.updateNString(2, "f" + fourByteUnicode + "14");
    rs.updateRow();
    assertEquals("f" + fourByteUnicode + "14", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "12", rs.getString(2));
    rs.updateNString("t1", "http://f" + fourByteUnicode + "15");
    rs.updateRow();
    assertEquals("http://f" + fourByteUnicode + "15", rs.getString(2));

    rs = stmt.executeQuery("SELECT * FROM StringParamCodec ORDER BY id");
    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "12", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "125", rs.getString(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "12", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("e" + fourByteUnicode + "3", rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "3654", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "36", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "46", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "3654", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "36", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "46", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("f" + fourByteUnicode + "14", rs.getString(2));
    assertEquals("f" + fourByteUnicode + "14", rs.getObject(2, (Class<String>) null));

    assertTrue(rs.next());
    assertEquals(
        "http://f" + fourByteUnicode + "15", rs.getObject(2, (Map<String, Class<?>>) null));
    assertEquals(
        "http://f" + fourByteUnicode + "15", rs.getObject("t1", (Map<String, Class<?>>) null));
    Map<String, Class<?>> empty = new HashMap<>();
    assertEquals("http://f" + fourByteUnicode + "15", rs.getObject("t1", empty));
    assertEquals("http://f" + fourByteUnicode + "15", rs.getURL(2).toString());
    assertEquals("http://f" + fourByteUnicode + "15", rs.getURL("t1").toString());
  }

  @Test
  public void wrongUtf8String() throws SQLException {
    final byte[] utf8WrongBytes =
        new byte[] {(byte) 0xc2, (byte) 0f, (byte) 0xDB, (byte) 00, (byte) 0xE1, (byte) 00};
    String st = new String(utf8WrongBytes);

    wrongUtf8(sharedConn, st, 1);
    wrongUtf8(sharedConnBinary, st, 2);
  }

  private void wrongUtf8(Connection con, String wrong, int id) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("INSERT INTO StringCodecWrong VALUES ('" + wrong + "', " + id + ")");
    ResultSet rs = stmt.executeQuery("SELECT * FROM StringCodecWrong ORDER BY id");
    rs.next();
    assertEquals(wrong, rs.getString(1));
  }
}
