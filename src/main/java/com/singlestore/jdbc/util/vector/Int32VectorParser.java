package com.singlestore.jdbc.util.vector;

import com.singlestore.jdbc.client.util.VectorType;

public class Int32VectorParser extends VectorParser<int[]> {

  public static final Int32VectorParser INSTANCE = new Int32VectorParser();

  protected Int32VectorParser() {
    super(VectorType.I32);
  }

  @Override
  public int[] parseDefault(byte[] data, int length) {
    String[] values = parseVectorString(data, length);
    int[] integers = new int[length];
    for (int i = 0; i < length; i++) {
      try {
        integers[i] = Integer.parseInt(values[i]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid I32 number format at index " + i + ": " + values[i]);
      }
    }
    return integers;
  }

  @Override
  int[] parseIntegers(byte[] data, int length) {
    return parseDefault(data, length);
  }

  @Override
  long[] parseLongs(byte[] data, int length) {
    int[] integers = parseDefault(data, length);
    long[] longs = new long[integers.length];
    for (int i = 0; i < integers.length; i++) {
      longs[i] = integers[i];
    }
    return longs;
  }

  @Override
  float[] parseFloats(byte[] data, int length) {
    int[] integers = parseDefault(data, length);
    float[] floats = new float[integers.length];
    for (int i = 0; i < integers.length; i++) {
      floats[i] = (float) integers[i];
    }
    return floats;
  }

  @Override
  double[] parseDoubles(byte[] data, int length) {
    int[] integers = parseDefault(data, length);
    double[] doubles = new double[integers.length];
    for (int i = 0; i < integers.length; i++) {
      doubles[i] = integers[i];
    }
    return doubles;
  }
}
