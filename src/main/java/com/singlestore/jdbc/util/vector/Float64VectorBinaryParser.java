package com.singlestore.jdbc.util.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Float64VectorBinaryParser extends Float64VectorParser {

  public static final Float64VectorBinaryParser INSTANCE = new Float64VectorBinaryParser();

  protected Float64VectorBinaryParser() {}

  @Override
  public double[] parseDefault(byte[] data, int length) {
    if (data.length != length * Double.BYTES) {
      throw new IllegalStateException(
          String.format(
              "Expected byte array of length %d (for %d F64), but got %d bytes.",
              length * Double.BYTES, length, data.length));
    }
    double[] doubles = new double[length];
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < length; i++) {
      doubles[i] = buffer.getDouble();
    }
    return doubles;
  }

  @Override
  String[] parseStrings(byte[] data, int length) {
    double[] doubles = parseDefault(data, length);
    String[] stringArray = new String[length];
    for (int i = 0; i < doubles.length; i++) {
      stringArray[i] = Double.toString(doubles[i]);
    }
    return stringArray;
  }
}
