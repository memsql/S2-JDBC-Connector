// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.plugin.codec;

import static com.singlestore.jdbc.plugin.codec.ByteArrayCodec.BINARY_PREFIX;

import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.plugin.Codec;
import com.singlestore.jdbc.type.Vector;
import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;

public class VectorCodec implements Codec<Vector> {

  public static final VectorCodec INSTANCE = new VectorCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.FLOAT64_VECTOR,
          DataType.FLOAT32_VECTOR,
          DataType.INT64_VECTOR,
          DataType.INT32_VECTOR,
          DataType.INT16_VECTOR,
          DataType.INT8_VECTOR);

  @Override
  public String className() {
    return Vector.class.getName();
  }

  @Override
  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Vector.class);
  }

  @Override
  public boolean canEncode(Object value) {
    return value instanceof Vector;
  }

  @Override
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return canEncode(value) ? String.valueOf(value).length() * 3 : -1;
  }

  @Override
  public Vector decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    if (COMPATIBLE_TYPES.contains(column.getType()) && column.getExtTypeFormat() != null) {
      byte[] arr = new byte[length.get()];
      buf.readBytes(arr);
      int dimensions = Integer.parseInt(column.getExtTypeFormat().split(",")[0]);
      return Vector.fromData(arr, dimensions, column.getType(), column.isBinary());
    }
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Vector", column.getType()));
  }

  @Override
  public Vector decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return decodeText(buf, length, column, cal);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    if (!(value instanceof Vector)) {
      throw new SQLDataException(
          String.format("Data type %s cannot be encoded as Vector text", value.getClass()));
    }
    Vector vector = (Vector) value;
    if (vector.isBinary()) {
      byte[] b = vector.getValues();
      int length = b.length;
      encoder.writeBytes(BINARY_PREFIX);
      encoder.writeBytesEscaped(
          b, maxLength == null ? length : Math.min(length, maxLength.intValue()));
      encoder.writeByte('\'');
    } else {
      encoder.writeByte('\'');
      encoder.writeAscii(vector.stringValue());
      encoder.writeByte('\'');
    }
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    if (!(value instanceof Vector)) {
      throw new SQLDataException(
          String.format("Data type %s cannot be encoded as Vector binary", value.getClass()));
    }
    Vector vector = (Vector) value;
    byte[] b = vector.getValues();
    int len = maxLength != null ? Math.min(maxLength.intValue(), b.length) : b.length;
    encoder.writeLength(len);
    encoder.writeBytes(b, 0, len);
  }

  @Override
  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
