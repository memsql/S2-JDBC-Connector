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
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/** Column metadata definition */
public class UnsignedBigIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * BIGINT UNSIGNED metadata type decoder
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
  public UnsignedBigIntColumn(
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
  protected UnsignedBigIntColumn(UnsignedBigIntColumn prev) {
    super(prev, true);
  }

  @Override
  public UnsignedBigIntColumn useAliasAsName() {
    return new UnsignedBigIntColumn(this);
  }

  @Override
  public String defaultClassname(Configuration conf) {
    return BigInteger.class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    return Types.BIGINT;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    return dataType.name() + " UNSIGNED";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return new BigInteger(buf.readAscii(length.get()));
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb);
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String s = buf.readAscii(length.get());
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return buf.readLong() != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = buf.atoull(length.get());
    if ((byte) result != result || result < 0) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    BigInteger val = new BigInteger(1, bb);
    try {
      return val.byteValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", val, dataType));
    }
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).toString();
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = buf.atoull(length.get());
    if ((short) result != result || result < 0) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = buf.readLong();
    if ((short) result != result || result < 0) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = buf.atoull(length.get());
    int res = (int) result;
    if (res != result || result < 0) {
      throw new SQLDataException("int overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {

    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    BigInteger val = new BigInteger(1, bb);
    try {
      return val.intValueExact();
    } catch (ArithmeticException ae) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", val));
    }
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (length.get() < 10) return buf.atoull(length.get());
    BigInteger val = new BigInteger(buf.readAscii(length.get()));
    try {
      return val.longValueExact();
    } catch (ArithmeticException ae) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
    }
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if ((buf.getByte(buf.pos() + 7) & 0x80) == 0) {
      return buf.readLong();
    } else {
      // error too big to return a long
      byte[] bb = new byte[8];
      for (int i = 7; i >= 0; i--) {
        bb[i] = buf.readByte();
      }
      BigInteger val = new BigInteger(1, bb);
      try {
        return val.longValueExact();
      } catch (ArithmeticException ae) {
        throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
      }
    }
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length.get()));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).floatValue();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length.get()));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).doubleValue();
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
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Time decodeTimeBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }
}
