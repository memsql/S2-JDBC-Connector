// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.column;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import com.singlestore.jdbc.plugin.codec.LocalTimeCodec;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/** Column metadata definition */
public class TimeColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * TIME metadata type decoder
   *
   * @param buf buffer
   * @param charset charset
   * @param length maximum data length
   * @param dataType data type
   * @param decimals decimal length
   * @param flags flags
   * @param stringPos string offset position in buffer
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public TimeColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    super(
        buf,
        charset,
        length,
        dataType,
        decimals,
        flags,
        stringPos,
        extTypeName,
        extTypeFormat,
        false);
  }

  /**
   * Recreate new column using alias as name.
   *
   * @param prev current column
   */
  protected TimeColumn(TimeColumn prev) {
    super(prev, true);
  }

  @Override
  public TimeColumn useAliasAsName() {
    return new TimeColumn(this);
  }

  @Override
  public String defaultClassname(Configuration conf) {
    return Time.class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    return Types.TIME;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    return dataType.name();
  }

  @Override
  public int getPrecision() {
    return getDisplaySize();
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return decodeTimeText(buf, length, null);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return decodeTimeBinary(buf, length, null);
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal)
      throws SQLDataException {

    if (length.get() == 0) {
      return createZeroTimeString();
    }

    boolean negate = buf.readByte() == 0x01;
    long days = 0;
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    if (length.get() > 4) {
      days = buf.readUnsignedInt();
      if (length.get() > 7) {
        hours = buf.readByte();
        minutes = buf.readByte();
        seconds = buf.readByte();
        if (length.get() > 8) {
          microseconds = buf.readInt();
        }
      }
    }

    String timeString = formatBasicTimeString(negate, days, hours, minutes, seconds);

    return formatWithMicroseconds(timeString, microseconds);
  }

  private String createZeroTimeString() {
    StringBuilder zeroValue = new StringBuilder("00:00:00");
    if (getDecimals() > 0) {
      zeroValue.append(".");
      for (int i = 0; i < getDecimals(); i++) zeroValue.append("0");
    }
    return zeroValue.toString();
  }

  private String formatBasicTimeString(
      boolean negate, long days, int hours, int minutes, int seconds) {
    int totalHours = (int) (days * 24 + hours);

    return String.format("%s%02d:%02d:%02d", negate ? "-" : "", totalHours, minutes, seconds);
  }

  private String formatWithMicroseconds(String timeString, long microseconds) {
    if (getDecimals() == 0) {
      if (microseconds == 0) {
        return timeString;
      }
      return timeString + "." + padZeros(microseconds, 6);
    }

    return timeString + "." + padZeros(microseconds, getDecimals());
  }

  private String padZeros(long number, int targetLength) {
    StringBuilder result = new StringBuilder(String.valueOf(number));
    while (result.length() < targetLength) {
      result.insert(0, "0");
    }
    return result.toString();
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public Date decodeDateText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Time decodeTimeText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    Calendar c = cal == null ? Calendar.getInstance() : cal;
    int offset = c.getTimeZone().getOffset(0);
    int[] parts = LocalTimeCodec.parseTime(buf, length, this, true);
    long timeInMillis =
        (parts[1] * 3_600_000L + parts[2] * 60_000L + parts[3] * 1_000L + parts[4] / 1_000_000)
                * parts[0]
            - offset;
    return new Time(timeInMillis);
  }

  @Override
  public Time decodeTimeBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    long dayOfMonth = 0;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;
    // specific case for TIME, to handle value not in 00:00:00-23:59:59
    int offset = cal.getTimeZone().getOffset(0);

    if (buf.pos() >= buf.buf().length - 1) {
      // If time is coming as '00:00:00' then corresponding byte value is null. Hence need to
      // pass default Time value in this case.
      return new Time(-offset);
    }

    boolean negate = buf.readByte() == 1;
    dayOfMonth = buf.readUnsignedInt();
    hour = buf.readByte();
    minutes = buf.readByte();
    seconds = buf.readByte();
    if (length.get() > 8) {
      microseconds = buf.readUnsignedInt();
    }
    long timeInMillis =
        ((24 * dayOfMonth + hour) * 3_600_000
                    + minutes * 60_000
                    + seconds * 1_000
                    + microseconds / 1_000)
                * (negate ? -1 : 1)
            - offset;
    return new Time(timeInMillis);
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    int[] parts = LocalTimeCodec.parseTime(buf, length, this, true);
    Timestamp t;

    // specific case for TIME, to handle value not in 00:00:00-23:59:59
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    synchronized (cal) {
      cal.clear();
      cal.setLenient(true);
      if (parts[0] == -1) {
        cal.set(
            1970, Calendar.JANUARY, 1, -parts[1], -parts[2], -parts[3] - (parts[4] > 0 ? 1 : 0));
        t = new Timestamp(cal.getTimeInMillis());
        if (parts[4] > 0) {
          t.setNanos(1_000_000_000 - parts[4]);
        }
      } else {
        cal.set(1970, Calendar.JANUARY, 1, parts[1], parts[2], parts[3]);
        t = new Timestamp(cal.getTimeInMillis());
        t.setNanos(parts[4]);
      }
    }
    return t;
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    long microseconds = 0;

    // specific case for TIME, to handle value not in 00:00:00-23:59:59
    boolean negate = buf.readByte() == 1;
    long dayOfMonth = buf.readUnsignedInt();
    int hour = buf.readByte();
    int minutes = buf.readByte();
    int seconds = buf.readByte();
    if (length.get() > 8) {
      microseconds = buf.readUnsignedInt();
    }
    int offset = cal.getTimeZone().getOffset(0);
    long timeInMillis =
        ((24 * dayOfMonth + hour) * 3_600_000
                    + minutes * 60_000
                    + seconds * 1_000
                    + microseconds / 1_000)
                * (negate ? -1 : 1)
            - offset;
    return new Timestamp(timeInMillis);
  }
}
