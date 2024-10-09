package com.singlestore.jdbc.util.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Float32VectorBinaryParser extends Float32VectorParser {

  public static final Float32VectorBinaryParser INSTANCE = new Float32VectorBinaryParser();

  protected Float32VectorBinaryParser() {}

  @Override
  public float[] parseDefault(byte[] data, int length) {
    if (data.length != length * Float.BYTES) {
      throw new IllegalStateException(
          String.format(
              "Expected byte array of length %d (for %d F32), but got %d bytes.",
              length * Float.BYTES, length, data.length));
    }
    float[] floats = new float[length];
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < length; i++) {
      floats[i] = buffer.getFloat();
    }
    return floats;
  }

  @Override
  String[] parseStrings(byte[] data, int length) {
    float[] floats = parseDefault(data, length);
    String[] stringArray = new String[length];
    for (int i = 0; i < floats.length; i++) {
      stringArray[i] = Float.toString(floats[i]);
    }
    return stringArray;
  }
}
