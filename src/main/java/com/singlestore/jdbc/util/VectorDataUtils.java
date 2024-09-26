package com.singlestore.jdbc.util;

import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.util.VectorType;
import java.nio.charset.StandardCharsets;

public final class VectorDataUtils {
  private VectorDataUtils() {}

  public static <T> T parse(byte[] data, int length, Class<T> parsedClass, DataType dataType) {
    return getParser(dataType).parse(data, length, parsedClass);
  }

  private static Parser getParser(DataType dataType) {
    switch (dataType) {
      case FLOAT64_VECTOR:
        return new Float64Parser();
      case FLOAT32_VECTOR:
        return new Float32Parser();
      case INT64_VECTOR:
        return new Int64Parser();
      case INT32_VECTOR:
        return new Int32Parser();
      case INT16_VECTOR:
        return new Int16Parser();
      case INT8_VECTOR:
        return new Int8Parser();
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + dataType.name());
    }
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

  private static class Float64Parser extends Parser {

    public Float64Parser() {
      super(VectorType.F64);
    }

    private double[] getDoubles(byte[] data, int length) {
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
      return getDoubles(data, length);
    }
  }

  private static class Float32Parser extends Parser {

    protected Float32Parser() {
      super(VectorType.F32);
    }

    protected float[] getFloats(byte[] data, int length) {
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
      return getFloats(data, length);
    }

    @Override
    double[] parseDoubles(byte[] data, int length) {
      float[] floats = getFloats(data, length);
      double[] doubles = new double[floats.length];
      for (int i = 0; i < floats.length; i++) {
        doubles[i] = floats[i];
      }
      return doubles;
    }
  }

  // Parser for 64-bit integer data
  private static class Int64Parser extends Parser {

    protected Int64Parser() {
      super(VectorType.I64);
    }

    private long[] getLongs(byte[] data, int length) {
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
      return getLongs(data, length);
    }

    @Override
    double[] parseDoubles(byte[] data, int length) {
      long[] longs = getLongs(data, length);
      double[] doubles = new double[longs.length];
      for (int i = 0; i < longs.length; i++) {
        doubles[i] = (double) longs[i];
      }
      return doubles;
    }
  }

  private static class Int32Parser extends Parser {

    protected Int32Parser() {
      super(VectorType.I32);
    }

    private int[] getIntegers(byte[] data, int length) {
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
      return getIntegers(data, length);
    }

    @Override
    long[] parseLongs(byte[] data, int length) {
      int[] integers = getIntegers(data, length);
      long[] longs = new long[integers.length];
      for (int i = 0; i < integers.length; i++) {
        longs[i] = integers[i];
      }
      return longs;
    }

    @Override
    float[] parseFloats(byte[] data, int length) {
      int[] integers = getIntegers(data, length);
      float[] floats = new float[integers.length];
      for (int i = 0; i < integers.length; i++) {
        floats[i] = (float) integers[i];
      }
      return floats;
    }

    @Override
    double[] parseDoubles(byte[] data, int length) {
      int[] integers = getIntegers(data, length);
      double[] doubles = new double[integers.length];
      for (int i = 0; i < integers.length; i++) {
        doubles[i] = integers[i];
      }
      return doubles;
    }
  }

  private static class Int16Parser extends Parser {

    protected Int16Parser() {
      super(VectorType.I16);
    }

    private short[] getShorts(byte[] data, int length) {
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
      return getShorts(data, length);
    }

    @Override
    int[] parseIntegers(byte[] data, int length) {
      short[] shorts = getShorts(data, length);
      int[] integers = new int[shorts.length];
      for (int i = 0; i < shorts.length; i++) {
        integers[i] = shorts[i];
      }
      return integers;
    }

    @Override
    long[] parseLongs(byte[] data, int length) {
      short[] shorts = getShorts(data, length);
      long[] longs = new long[shorts.length];
      for (int i = 0; i < shorts.length; i++) {
        longs[i] = shorts[i];
      }
      return longs;
    }

    @Override
    float[] parseFloats(byte[] data, int length) {
      short[] shorts = getShorts(data, length);
      float[] floats = new float[shorts.length];
      for (int i = 0; i < shorts.length; i++) {
        floats[i] = shorts[i];
      }
      return floats;
    }

    @Override
    double[] parseDoubles(byte[] data, int length) {
      short[] shorts = getShorts(data, length);
      double[] doubles = new double[shorts.length];
      for (int i = 0; i < shorts.length; i++) {
        doubles[i] = shorts[i];
      }
      return doubles;
    }
  }

  private static class Int8Parser extends Parser {

    protected Int8Parser() {
      super(VectorType.I8);
    }

    private byte[] getBytes(byte[] data, int length) {
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
      byte[] bytes = getBytes(data, length);
      short[] shorts = new short[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
        shorts[i] = bytes[i];
      }
      return shorts;
    }

    @Override
    int[] parseIntegers(byte[] data, int length) {
      byte[] bytes = getBytes(data, length);
      int[] integers = new int[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
        integers[i] = bytes[i];
      }
      return integers;
    }

    @Override
    long[] parseLongs(byte[] data, int length) {
      byte[] bytes = getBytes(data, length);
      long[] longs = new long[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
        longs[i] = bytes[i];
      }
      return longs;
    }

    @Override
    float[] parseFloats(byte[] data, int length) {
      byte[] bytes = getBytes(data, length);
      float[] floats = new float[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
        floats[i] = bytes[i];
      }
      return floats;
    }

    @Override
    double[] parseDoubles(byte[] data, int length) {
      byte[] bytes = getBytes(data, length);
      double[] doubles = new double[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
        doubles[i] = bytes[i];
      }
      return doubles;
    }

    @Override
    byte[] parseBytes(byte[] data, int length) {
      return getBytes(data, length);
    }
  }

  private abstract static class Parser {

    private final VectorType type;

    protected Parser(VectorType type) {
      this.type = type;
    }

    private <T> T parse(byte[] data, int length, Class<T> parsedClass) {
      if (parsedClass == double[].class) {
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
  }
}
