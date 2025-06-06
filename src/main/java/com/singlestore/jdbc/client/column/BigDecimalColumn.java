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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/** Column metadata definition */
public class BigDecimalColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * Constructor for column corresponding to DECIMAL datatype. Class permit specific decoding for
   * this datatype
   *
   * @param buf Column definition MySQL packet buffer
   * @param charset charset
   * @param length datatype length
   * @param dataType data type
   * @param decimals number of decimals
   * @param flags column flags
   * @param stringPos string value position
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public BigDecimalColumn(
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
  protected BigDecimalColumn(BigDecimalColumn prev) {
    super(prev, true);
  }

  @Override
  public BigDecimalColumn useAliasAsName() {
    return new BigDecimalColumn(this);
  }

  @Override
  public String defaultClassname(Configuration conf) {
    return BigDecimal.class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    return Types.DECIMAL;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    if (isSigned()) return dataType.name();
    return dataType.name() + " UNSIGNED";
  }

  @Override
  public int getPrecision() {
    // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
    // so :
    // - if can be signed, 1 byte is saved for sign
    // - if decimal > 0, one byte more for dot
    if (isSigned()) {
      return (int) columnLength - ((decimals > 0) ? 2 : 1);
    } else {
      return (int) columnLength - ((decimals > 0) ? 1 : 0);
    }
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get()));
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get()));
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get())).intValue() != 0;
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return decodeBooleanText(buf, length);
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", str, dataType));
    }
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result;
    String str = buf.readString(length.get());
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Short", str));
    }
    if ((short) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeShortText(buf, length);
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result;
    String str = buf.readString(length.get());
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
    }
    int res = (int) result;
    if (res != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeIntText(buf, length);
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str2 = buf.readAscii(length.get());
    try {
      return new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str2));
    }
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
    }
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Float.valueOf(buf.readAscii(length.get()));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get())).floatValue();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Double.valueOf(buf.readAscii(length.get()));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get())).doubleValue();
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
