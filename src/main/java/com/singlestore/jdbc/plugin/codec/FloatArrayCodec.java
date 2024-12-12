// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.
package com.singlestore.jdbc.plugin.codec;

import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.plugin.Codec;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.EnumSet;

/** Float codec */
public class FloatArrayCodec implements Codec<float[]> {

  /** default instance */
  public static final FloatArrayCodec INSTANCE = new FloatArrayCodec();

  private static Class<?> floatArrayClass = Array.newInstance(float.class, 0).getClass();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.VARCHAR);

  public String className() {
    return float[].class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((!type.isPrimitive() && type == floatArrayClass && type.isArray()));
  }

  public boolean canEncode(Object value) {
    return value instanceof float[];
  }

  @Override
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return canEncode(value) ? Arrays.toString((float[]) value).getBytes().length : -1;
  }

  @Override
  public float[] decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {

    return toFloatArray(getBytes(buf, length, column));
  }

  @Override
  public float[] decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {

    return toFloatArray(getBytes(buf, length, column));
  }

  static final int BYTES_IN_FLOAT = Float.SIZE / Byte.SIZE;

  public static byte[] toByteArray(float[] floatArray) {
    ByteBuffer buffer = ByteBuffer.allocate(floatArray.length * BYTES_IN_FLOAT);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.asFloatBuffer().put(floatArray);
    return buffer.array();
  }

  public static float[] toFloatArray(byte[] byteArray) {
    float[] result = new float[byteArray.length / BYTES_IN_FLOAT];
    ByteBuffer.wrap(byteArray)
        .order(ByteOrder.LITTLE_ENDIAN)
        .asFloatBuffer()
        .get(result, 0, result.length);
    return result;
  }

  private byte[] getBytes(ReadableByteBuf buf, MutableInt length, ColumnDecoder column)
      throws SQLDataException {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case VARCHAR:
      case GEOMETRY:
        byte[] arr = new byte[length.get()];
        buf.readBytes(arr);
        return arr;

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as float[]", column.getType()));
    }
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    byte[] encoded = toByteArray((float[]) value);
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    encoder.writeBytesEscaped(encoded, encoded.length);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(
      final Writer encoder, final Object value, final Calendar cal, final Long maxLength)
      throws IOException {
    byte[] arr = toByteArray((float[]) value);
    encoder.writeLength(arr.length);
    encoder.writeBytes(arr);
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
