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
import java.util.Calendar;
import java.util.EnumSet;

public class DoubleCodec implements Codec<Double> {

  /** default instance */
  public static final DoubleCodec INSTANCE = new DoubleCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Double.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Double.TYPE) || type.isAssignableFrom(Double.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Double;
  }

  @Override
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return canEncode(value) ? String.valueOf(value).getBytes().length : -1;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Double decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return column.decodeDoubleText(buf, length);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Double decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return column.decodeDoubleBinary(buf, length);
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeDouble((Double) value);
  }

  public int getBinaryEncodeType() {
    return DataType.DOUBLE.get();
  }
}
