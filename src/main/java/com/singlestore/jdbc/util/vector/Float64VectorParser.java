package com.singlestore.jdbc.util.vector;

import com.singlestore.jdbc.client.util.VectorType;

public class Float64VectorParser extends VectorParser<double[]> {

  public static final Float64VectorParser INSTANCE = new Float64VectorParser();

  protected Float64VectorParser() {
    super(VectorType.F64);
  }

  @Override
  public double[] parseDefault(byte[] data, int length) {
    String[] values = parseVectorString(data, length);
    double[] doubles = new double[length];
    for (int i = 0; i < length; i++) {
      try {
        doubles[i] = Double.parseDouble(values[i]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid F64 number format at index " + i + ": " + values[i]);
      }
    }
    return doubles;
  }

  @Override
  double[] parseDoubles(byte[] data, int length) {
    return parseDefault(data, length);
  }
}
