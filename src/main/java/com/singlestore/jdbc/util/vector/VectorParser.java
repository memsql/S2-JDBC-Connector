package com.singlestore.jdbc.util.vector;

import com.singlestore.jdbc.client.util.VectorType;
import java.nio.charset.StandardCharsets;

public abstract class VectorParser<M> {

  private final VectorType type;

  public VectorParser(VectorType type) {
    this.type = type;
  }

  @SuppressWarnings("unchecked")
  public <T> T parse(byte[] data, int length, Class<T> parsedClass) {
    if (parsedClass == String[].class) {
      return (T) this.parseStrings(data, length);
    } else if (parsedClass == double[].class) {
      return (T) this.parseDoubles(data, length);
    } else if (parsedClass == float[].class) {
      return (T) this.parseFloats(data, length);
    } else if (parsedClass == short[].class) {
      return (T) this.parseShorts(data, length);
    } else if (parsedClass == int[].class) {
      return (T) this.parseIntegers(data, length);
    } else if (parsedClass == long[].class) {
      return (T) this.parseLongs(data, length);
    } else if (parsedClass == byte[].class) {
      return (T) this.parseBytes(data, length);
    } else {
      throw new IllegalStateException(parsedClass.getSimpleName());
    }
  }

  public abstract M parseDefault(byte[] data, int length);

  public VectorType getType() {
    return type;
  }

  String[] parseStrings(byte[] data, int length) {
    return parseVectorString(data, length);
  }

  short[] parseShorts(byte[] data, int length) {
    throw new UnsupportedOperationException(
        "Unable to convert Vector of " + type.name() + " elements to short array.");
  }

  int[] parseIntegers(byte[] data, int length) {
    throw new UnsupportedOperationException(
        "Unable to convert Vector of " + type.name() + " elements to int array.");
  }

  long[] parseLongs(byte[] data, int length) {
    throw new UnsupportedOperationException(
        "Unable to convert Vector of " + type.name() + " elements to long array.");
  }

  float[] parseFloats(byte[] data, int length) {
    throw new UnsupportedOperationException(
        "Unable to convert Vector of " + type.name() + " elements to float array.");
  }

  double[] parseDoubles(byte[] data, int length) {
    throw new UnsupportedOperationException(
        "Unable to convert Vector of " + type.name() + " elements to double array.");
  }

  byte[] parseBytes(byte[] data, int length) {
    throw new UnsupportedOperationException(
        "Unable to convert Vector of " + type.name() + " elements to byte array.");
  }

  public static String[] parseVectorString(byte[] input, Integer length) {
    String str = new String(input, StandardCharsets.UTF_8).replaceAll("[\\[\\]]", "").trim();
    String[] values = str.split("\\s*,\\s*");
    if (length != null && values.length != length) {
      throw new IllegalStateException(
          "Expected vector length: " + length + ", but got: " + values.length + ".");
    }
    return values;
  }
}
