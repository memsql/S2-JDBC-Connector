package com.singlestore.jdbc.util.vector;

import com.singlestore.jdbc.client.util.VectorType;

public class Int16VectorParser extends VectorParser<short[]> {

  public static final Int16VectorParser INSTANCE = new Int16VectorParser();

  protected Int16VectorParser() {
    super(VectorType.I16);
  }

  @Override
  public short[] parseDefault(byte[] data, int length) {
    String[] values = parseVectorString(data, length);
    short[] shorts = new short[length];
    for (int i = 0; i < length; i++) {
      try {
        shorts[i] = Short.parseShort(values[i]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid I16 number format at index " + i + ": " + values[i]);
      }
    }
    return shorts;
  }

  @Override
  short[] parseShorts(byte[] data, int length) {
    return parseDefault(data, length);
  }

  @Override
  int[] parseIntegers(byte[] data, int length) {
    short[] shorts = parseDefault(data, length);
    int[] integers = new int[shorts.length];
    for (int i = 0; i < shorts.length; i++) {
      integers[i] = shorts[i];
    }
    return integers;
  }

  @Override
  long[] parseLongs(byte[] data, int length) {
    short[] shorts = parseDefault(data, length);
    long[] longs = new long[shorts.length];
    for (int i = 0; i < shorts.length; i++) {
      longs[i] = shorts[i];
    }
    return longs;
  }

  @Override
  float[] parseFloats(byte[] data, int length) {
    short[] shorts = parseDefault(data, length);
    float[] floats = new float[shorts.length];
    for (int i = 0; i < shorts.length; i++) {
      floats[i] = shorts[i];
    }
    return floats;
  }

  @Override
  double[] parseDoubles(byte[] data, int length) {
    short[] shorts = parseDefault(data, length);
    double[] doubles = new double[shorts.length];
    for (int i = 0; i < shorts.length; i++) {
      doubles[i] = shorts[i];
    }
    return doubles;
  }
}
