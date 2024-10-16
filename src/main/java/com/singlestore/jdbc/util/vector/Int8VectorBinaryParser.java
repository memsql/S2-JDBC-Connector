package com.singlestore.jdbc.util.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Int8VectorBinaryParser extends Int8VectorParser {

  public static final Int8VectorBinaryParser INSTANCE = new Int8VectorBinaryParser();

  protected Int8VectorBinaryParser() {}

  @Override
  public byte[] parseDefault(byte[] data, int length) {
    if (data.length != length * Byte.BYTES) {
      throw new IllegalStateException(
          String.format(
              "Expected byte array of length %d (for %d I8), but got %d bytes.",
              length * Byte.BYTES, length, data.length));
    }
    byte[] bytes = new byte[length];
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < length; i++) {
      bytes[i] = buffer.get(i);
    }
    return bytes;
  }

  @Override
  String[] parseStrings(byte[] data, int length) {
    byte[] bytes = parseDefault(data, length);
    String[] stringArray = new String[length];
    for (int i = 0; i < bytes.length; i++) {
      stringArray[i] = Byte.toString(bytes[i]);
    }
    return stringArray;
  }
}
