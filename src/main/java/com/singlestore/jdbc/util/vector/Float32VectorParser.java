package com.singlestore.jdbc.util.vector;

import com.singlestore.jdbc.client.util.VectorType;

public class Float32VectorParser extends VectorParser<float[]> {

  public static final Float32VectorParser INSTANCE = new Float32VectorParser();

  protected Float32VectorParser() {
    super(VectorType.F32);
  }

  @Override
  public float[] parseDefault(byte[] data, int length) {
    String[] values = parseVectorString(data, length);
    float[] floats = new float[length];
    for (int i = 0; i < length; i++) {
      try {
        floats[i] = Float.parseFloat(values[i]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid F32 number format at index " + i + ": " + values[i]);
      }
    }
    return floats;
  }

  @Override
  float[] parseFloats(byte[] data, int length) {
    return parseDefault(data, length);
  }

  @Override
  double[] parseDoubles(byte[] data, int length) {
    float[] floats = parseDefault(data, length);
    double[] doubles = new double[floats.length];
    for (int i = 0; i < floats.length; i++) {
      doubles[i] = floats[i];
    }
    return doubles;
  }
}
