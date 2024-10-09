package com.singlestore.jdbc.util.vector;

import com.singlestore.jdbc.client.util.VectorType;

public class Int64VectorParser extends VectorParser<long[]> {

  public static final Int64VectorParser INSTANCE = new Int64VectorParser();

  protected Int64VectorParser() {
    super(VectorType.I64);
  }

  @Override
  public long[] parseDefault(byte[] data, int length) {
    String[] values = parseVectorString(data, length);
    long[] longs = new long[length];
    for (int i = 0; i < length; i++) {
      try {
        longs[i] = Long.parseLong(values[i]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid I64 number format at index " + i + ": " + values[i]);
      }
    }
    return longs;
  }

  @Override
  long[] parseLongs(byte[] data, int length) {
    return parseDefault(data, length);
  }

  @Override
  double[] parseDoubles(byte[] data, int length) {
    long[] longs = parseDefault(data, length);
    double[] doubles = new double[longs.length];
    for (int i = 0; i < longs.length; i++) {
      doubles[i] = (double) longs[i];
    }
    return doubles;
  }
}
