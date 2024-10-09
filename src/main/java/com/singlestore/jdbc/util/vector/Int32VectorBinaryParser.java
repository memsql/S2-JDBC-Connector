package com.singlestore.jdbc.util.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Int32VectorBinaryParser extends Int32VectorParser {

  public static final Int32VectorBinaryParser INSTANCE = new Int32VectorBinaryParser();

  protected Int32VectorBinaryParser() {}

  @Override
  public int[] parseDefault(byte[] data, int length) {
    if (data.length != length * Integer.BYTES) {
      throw new IllegalStateException(
          String.format(
              "Expected byte array of length %d (for %d I32), but got %d bytes.",
              length * Integer.BYTES, length, data.length));
    }
    int[] integers = new int[length];
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < length; i++) {
      integers[i] = buffer.getInt();
    }
    return integers;
  }

  @Override
  String[] parseStrings(byte[] data, int length) {
    int[] integers = parseDefault(data, length);
    String[] stringArray = new String[length];
    for (int i = 0; i < integers.length; i++) {
      stringArray[i] = Integer.toString(integers[i]);
    }
    return stringArray;
  }
}
