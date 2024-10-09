package com.singlestore.jdbc.util.vector;

import com.singlestore.jdbc.client.util.VectorType;

public class Int8VectorParser extends VectorParser<byte[]> {

  public static final Int8VectorParser INSTANCE = new Int8VectorParser();

  protected Int8VectorParser() {
    super(VectorType.I8);
  }

  @Override
  public byte[] parseDefault(byte[] data, int length) {
    String[] values = parseVectorString(data, length);
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      try {
        bytes[i] = Byte.parseByte(values[i]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid I8 number format at index " + i + ": " + values[i]);
      }
    }
    return bytes;
  }

  @Override
  short[] parseShorts(byte[] data, int length) {
    byte[] bytes = parseDefault(data, length);
    short[] shorts = new short[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      shorts[i] = bytes[i];
    }
    return shorts;
  }

  @Override
  int[] parseIntegers(byte[] data, int length) {
    byte[] bytes = parseDefault(data, length);
    int[] integers = new int[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      integers[i] = bytes[i];
    }
    return integers;
  }

  @Override
  long[] parseLongs(byte[] data, int length) {
    byte[] bytes = parseDefault(data, length);
    long[] longs = new long[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      longs[i] = bytes[i];
    }
    return longs;
  }

  @Override
  float[] parseFloats(byte[] data, int length) {
    byte[] bytes = parseDefault(data, length);
    float[] floats = new float[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      floats[i] = bytes[i];
    }
    return floats;
  }

  @Override
  double[] parseDoubles(byte[] data, int length) {
    byte[] bytes = parseDefault(data, length);
    double[] doubles = new double[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      doubles[i] = bytes[i];
    }
    return doubles;
  }

  @Override
  byte[] parseBytes(byte[] data, int length) {
    return parseDefault(data, length);
  }
}
