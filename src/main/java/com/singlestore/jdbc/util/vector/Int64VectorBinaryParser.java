package com.singlestore.jdbc.util.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Int64VectorBinaryParser extends Int64VectorParser {

  public static final Int64VectorBinaryParser INSTANCE = new Int64VectorBinaryParser();

  protected Int64VectorBinaryParser() {}

  @Override
  public long[] parseDefault(byte[] data, int length) {
    if (data.length != length * Long.BYTES) {
      throw new IllegalStateException(
          String.format(
              "Expected byte array of length %d (for %d I64), but got %d bytes.",
              length * Long.BYTES, length, data.length));
    }
    long[] longs = new long[length];
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < length; i++) {
      longs[i] = buffer.getLong();
    }
    return longs;
  }

  @Override
  String[] parseStrings(byte[] data, int length) {
    long[] longs = parseDefault(data, length);
    String[] stringArray = new String[length];
    for (int i = 0; i < longs.length; i++) {
      stringArray[i] = Long.toString(longs[i]);
    }
    return stringArray;
  }
}
