// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.codec.list;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.codec.Codec;
import com.singlestore.jdbc.codec.DataType;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.UUID;

/** UUID codec */
public class UuidCodec implements Codec<UUID> {

  /** default instance */
  public static final UuidCodec INSTANCE = new UuidCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.VARCHAR, DataType.CHAR);

  public String className() {
    return UUID.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(UUID.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof UUID;
  }

  public UUID decodeText(
      final ReadableByteBuf buf,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException {
    String val = buf.readString(length);
    try {
      return UUID.fromString(val);
    } catch (Throwable e) {
      // eat
    }
    throw new SQLDataException(
        String.format("value '%s' (%s) cannot be decoded as UUID", val, column.getType()));
  }

  public UUID decodeBinary(
      final ReadableByteBuf buf,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException {
    String val = buf.readString(length);
    try {
      return UUID.fromString(val);
    } catch (Throwable e) {
      // eat
    }
    throw new SQLDataException(
        String.format("value '%s' (%s) cannot be decoded as UUID", val, column.getType()));
  }

  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeAscii(value.toString());
    encoder.writeByte('\'');
  }

  public void encodeBinary(PacketWriter encoder, Object value, Calendar cal, Long length)
      throws IOException {
    String valueSt = value.toString();
    encoder.writeLength(valueSt.length());
    encoder.writeAscii(valueSt);
  }

  public int getBinaryEncodeType() {
    return DataType.VARCHAR.get();
  }
}
