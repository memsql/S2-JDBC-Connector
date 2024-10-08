package com.singlestore.jdbc.util.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Int16VectorBinaryParser extends Int16VectorParser {

  public static final Int16VectorBinaryParser INSTANCE = new Int16VectorBinaryParser();

  protected Int16VectorBinaryParser() {}

  @Override
  public short[] parseDefault(byte[] data, int length) {
    if (data.length != length * Short.BYTES) {
      throw new IllegalStateException(
          String.format(
              "Expected byte array of length %d (for %d I16), but got %d bytes.",
              length * Short.BYTES, length, data.length));
    }
    short[] shorts = new short[length];
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < length; i++) {
      shorts[i] = buffer.getShort();
    }
    return shorts;
  }

  @Override
  String[] parseStrings(byte[] data, int length) {
    short[] shorts = parseDefault(data, length);
    String[] stringArray = new String[length];
    for (int i = 0; i < shorts.length; i++) {
      stringArray[i] = Short.toString(shorts[i]);
    }
    return stringArray;
  }
}
