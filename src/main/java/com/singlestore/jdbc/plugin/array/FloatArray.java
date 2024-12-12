// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.
package com.singlestore.jdbc.plugin.array;

import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.util.constants.ColumnFlags;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Map;

public class FloatArray implements Array {

  private final float[] val;
  private Context context;

  public FloatArray(float[] val, Context context) {
    this.val = val;
    this.context = context;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    return "float[]";
  }

  @Override
  public int getBaseType() throws SQLException {
    return Types.FLOAT;
  }

  @Override
  public Object getArray() throws SQLException {
    return this.val;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getArray(Map<String, Class<?>> map) is not supported");
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    if (index < 1 || index > val.length) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, val.length));
    }
    if (count < 0 || (index - 1 + count) > val.length) {
      throw new SQLException(
          String.format(
              "Count value is too big. Count is %s but cannot be > to %s",
              count, val.length - (index - 1)));
    }

    return Arrays.copyOfRange(val, (int) index - 1, (int) (index - 1) + count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getArray(long index, int count, Map<String, Class<?>> map) is not supported");
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return getResultSet(1, this.val.length);
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getResultSet(Map<String, Class<?>> map) is not supported");
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    byte[][] rows = new byte[count][];
    for (int i = 0; i < count; i++) {
      byte[] val =
          Float.toString(this.val[(int) index - 1 + i]).getBytes(StandardCharsets.US_ASCII);
      rows[i] = new byte[val.length + 1];
      rows[i][0] = (byte) val.length;
      System.arraycopy(val, 0, rows[i], 1, val.length);
    }

    return new CompleteResult(
        new ColumnDecoder[] {ColumnDecoder.create("Array", DataType.FLOAT, ColumnFlags.NOT_NULL)},
        rows,
        context,
        ResultSet.TYPE_SCROLL_INSENSITIVE);
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getResultSet(long index, int count, Map<String, Class<?>> map) is not supported");
  }

  @Override
  public void free() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FloatArray that = (FloatArray) o;

    return Arrays.equals(val, that.val);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(val);
  }
}
