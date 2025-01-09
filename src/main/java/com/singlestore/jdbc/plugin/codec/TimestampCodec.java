// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.plugin.codec;

import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.plugin.Codec;
import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.EnumSet;

public class TimestampCodec implements Codec<Timestamp> {

  public static final TimestampCodec INSTANCE = new TimestampCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.TIME,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Timestamp.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Timestamp.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Timestamp || java.util.Date.class.equals(value.getClass());
  }

  @Override
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return 27;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Timestamp decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return column.decodeTimestampText(buf, length, cal);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Timestamp decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return column.decodeTimestampBinary(buf, length, cal);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar providedCal, Long maxLen)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(val);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);
    int microseconds = 0;
    if (val instanceof Timestamp) {
      microseconds = ((Timestamp) val).getNanos() / 1000;
    } else if (val instanceof java.util.Date) {
      microseconds = (int) ((((java.util.Date) val).getTime() % 1000) * 1000);
    }

    if (microseconds > 0) {
      if (microseconds % 1000 == 0) {
        encoder.writeAscii("." + Integer.toString(microseconds / 1000 + 1000).substring(1));
      } else {
        encoder.writeAscii("." + Integer.toString(microseconds + 1000000).substring(1));
      }
    }

    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar providedCal, Long maxLength)
      throws IOException {
    int microseconds = 0;
    long timeInMillis = 0;
    if (value instanceof Timestamp) {
      Timestamp ts = (Timestamp) value;
      microseconds = ts.getNanos() / 1000;
      timeInMillis = ts.getTime();
    } else if (value instanceof java.util.Date) {
      java.util.Date dt = (java.util.Date) value;
      timeInMillis = dt.getTime();
      microseconds = (int) ((timeInMillis % 1000) * 1000);
    }

    if (providedCal == null) {
      Calendar cal = Calendar.getInstance();
      cal.clear();
      cal.setTimeInMillis(timeInMillis);
      if (microseconds == 0) {
        encoder.writeByte(7); // length
        encoder.writeShort((short) cal.get(Calendar.YEAR));
        encoder.writeByte((cal.get(Calendar.MONTH) + 1));
        encoder.writeByte(cal.get(Calendar.DAY_OF_MONTH));
        encoder.writeByte(cal.get(Calendar.HOUR_OF_DAY));
        encoder.writeByte(cal.get(Calendar.MINUTE));
        encoder.writeByte(cal.get(Calendar.SECOND));
      } else {
        encoder.writeByte(11); // length
        encoder.writeShort((short) cal.get(Calendar.YEAR));
        encoder.writeByte((cal.get(Calendar.MONTH) + 1));
        encoder.writeByte(cal.get(Calendar.DAY_OF_MONTH));
        encoder.writeByte(cal.get(Calendar.HOUR_OF_DAY));
        encoder.writeByte(cal.get(Calendar.MINUTE));
        encoder.writeByte(cal.get(Calendar.SECOND));
        encoder.writeInt(microseconds);
      }
    } else {
      synchronized (providedCal) {
        providedCal.clear();
        providedCal.setTimeInMillis(timeInMillis);
        if (microseconds == 0) {
          encoder.writeByte(7); // length
          encoder.writeShort((short) providedCal.get(Calendar.YEAR));
          encoder.writeByte((providedCal.get(Calendar.MONTH) + 1));
          encoder.writeByte(providedCal.get(Calendar.DAY_OF_MONTH));
          encoder.writeByte(providedCal.get(Calendar.HOUR_OF_DAY));
          encoder.writeByte(providedCal.get(Calendar.MINUTE));
          encoder.writeByte(providedCal.get(Calendar.SECOND));
        } else {
          encoder.writeByte(11); // length
          encoder.writeShort((short) providedCal.get(Calendar.YEAR));
          encoder.writeByte((providedCal.get(Calendar.MONTH) + 1));
          encoder.writeByte(providedCal.get(Calendar.DAY_OF_MONTH));
          encoder.writeByte(providedCal.get(Calendar.HOUR_OF_DAY));
          encoder.writeByte(providedCal.get(Calendar.MINUTE));
          encoder.writeByte(providedCal.get(Calendar.SECOND));
          encoder.writeInt(microseconds);
        }
      }
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
