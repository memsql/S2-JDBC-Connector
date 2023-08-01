// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin;

import com.singlestore.jdbc.client.Column;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;

public interface Codec<T> {

  String className();

  boolean canDecode(Column column, Class<?> type);

  boolean canEncode(Object value);

  T decodeText(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException;

  T decodeBinary(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException;

  void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  void encodeBinary(Writer encoder, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  default boolean canEncodeLongData() {
    return false;
  }

  default void encodeLongData(Writer encoder, T value, Long length)
      throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  default byte[] encodeData(T value, Long length) throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  default void encodeBinaryAsString(Writer encoder, Object value, Long length) throws IOException {
    byte[] b = value.toString().getBytes(StandardCharsets.UTF_8);
    int len = length != null ? Math.min(length.intValue(), b.length) : b.length;
    encoder.writeLength(len);
    encoder.writeBytes(b, 0, len);
  }

  int getBinaryEncodeType();
}
