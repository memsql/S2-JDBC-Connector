// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

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
import java.util.Arrays;
import java.util.Calendar;
import java.util.EnumSet;

public class ByteArrayCodec implements Codec<byte[]> {

  public static final byte[] BINARY_PREFIX = {'_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\''};

  public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.BIT,
          DataType.GEOMETRY,
          DataType.VARCHAR,
          DataType.CHAR);

  public String className() {
    return byte[].class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  public boolean canEncode(Object value) {
    return value instanceof byte[];
  }

  @Override
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return canEncode(value) ? ((byte[]) value).length * 2 : -1;
  }

  @Override
  public byte[] decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return getBytes(buf, length, column);
  }

  private byte[] getBytes(ReadableByteBuf buf, MutableInt length, ColumnDecoder column)
      throws SQLDataException {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case CHAR:
      case VARCHAR:
      case GEOMETRY:
        byte[] arr = new byte[length.get()];
        buf.readBytes(arr);
        return arr;
      case BIT:
        return parseBit(buf, length);
      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as byte[]", column.getType()));
    }
  }

  /**
   * Reads BIT type as 8 bytes array and removes the leading zero bytes. SingleStore returns 8 bytes
   * array with leading zero bytes for BIT type.
   *
   * @param buf to read
   * @param length length of data
   * @return byte array of BIT column
   */
  public static byte[] parseBit(ReadableByteBuf buf, MutableInt length) {
    byte[] arr = new byte[length.get()];
    buf.readBytes(arr);
    // removes the leading zero bytes from array
    int index = 0;
    while (index < arr.length && arr[index] == (byte) 0) {
      index++;
    }
    if (arr.length == index) {
      return new byte[] {0};
    }
    return Arrays.copyOfRange(arr, index, arr.length);
  }

  @Override
  public byte[] decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return getBytes(buf, length, column);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    int length = ((byte[]) value).length;

    encoder.writeBytes(BINARY_PREFIX);
    encoder.writeBytesEscaped(
        ((byte[]) value), maxLength == null ? length : Math.min(length, maxLength.intValue()));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    int length = ((byte[]) value).length;
    if (maxLength != null) length = Math.min(length, maxLength.intValue());
    encoder.writeLength(length);
    encoder.writeBytes(((byte[]) value), 0, length);
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
