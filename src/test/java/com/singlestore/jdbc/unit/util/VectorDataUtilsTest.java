// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.integration.Common;
import com.singlestore.jdbc.util.VectorDataUtils;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class VectorDataUtilsTest {

  @Test
  public void parseVectorString() {
    int[] ints = new int[] {-1, 22, Integer.MAX_VALUE, -123213, Integer.MIN_VALUE, Byte.MAX_VALUE};
    String[] expectedArray =
        new String[] {"-1", "22", "2147483647", "-123213", "-2147483648", "127"};
    String data = Arrays.toString(ints);
    String[] actualStrings =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), 6, String[].class, DataType.INT32_VECTOR);
    assertArrayEquals(expectedArray, actualStrings);

    // binary
    byte[] binary = encodeIntArray(ints);
    actualStrings = VectorDataUtils.parseBinary(binary, 6, String[].class, DataType.INT32_VECTOR);
    assertArrayEquals(expectedArray, actualStrings);

    data = "    [   -1, 22,    2147483647,    -123213   ,   -2147483648,     127]     ";
    actualStrings =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), 6, String[].class, DataType.INT32_VECTOR);
    assertArrayEquals(expectedArray, actualStrings);
    Common.assertThrowsContains(
        IllegalStateException.class,
        () ->
            VectorDataUtils.parse(
                Arrays.toString(ints).getBytes(), 2, int[].class, DataType.INT32_VECTOR),
        "Expected vector length: 2, but got: 6.");

    Common.assertThrowsContains(
        IllegalArgumentException.class,
        () -> VectorDataUtils.parse("[1,3,4s]".getBytes(), 3, int[].class, DataType.INT32_VECTOR),
        "Invalid I32 number format at index 2: 4s");
  }

  @Test
  public void wrongDimensionsNumber() {
    int[] ints = new int[] {-1, 22, Byte.MAX_VALUE};
    String data = Arrays.toString(ints);
    Common.assertThrowsContains(
        IllegalStateException.class,
        () -> VectorDataUtils.parse(data.getBytes(), 2, int[].class, DataType.INT32_VECTOR),
        "Expected vector length: 2, but got: 3.");

    // binary
    byte[] binary = encodeIntArray(ints);
    Common.assertThrowsContains(
        IllegalStateException.class,
        () -> VectorDataUtils.parseBinary(binary, 2, int[].class, DataType.INT32_VECTOR),
        "Expected byte array of length 8 (for 2 I32), but got 12 bytes.");
  }

  @Test
  public void testInt8() {
    byte[] arr = new byte[] {-1, 22, Byte.MAX_VALUE};
    short[] shortArr = new short[] {-1, 22, Byte.MAX_VALUE};
    int[] intArr = new int[] {-1, 22, Byte.MAX_VALUE};
    long[] longArr = new long[] {-1, 22, Byte.MAX_VALUE};
    double[] doubleArr = new double[] {-1, 22, Byte.MAX_VALUE};
    float[] floatArr = new float[] {-1, 22, Byte.MAX_VALUE};

    String data = Arrays.toString(arr);
    byte[] actualArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, byte[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualArr, arr);

    short[] actualShortArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, short[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualShortArr, shortArr);

    int[] actualIntArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, int[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualIntArr, intArr);

    long[] actualLongArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, long[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualLongArr, longArr);

    double[] actualDoubleArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            double[].class,
            DataType.INT8_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    float[] actualFloatArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, float[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualFloatArr, floatArr);

    // wrong value
    int[] wrongIntArr = new int[] {-1, 22, Byte.MAX_VALUE + 1};
    String wrongData = Arrays.toString(wrongIntArr);
    Common.assertThrowsContains(
        IllegalArgumentException.class,
        () ->
            VectorDataUtils.parse(
                wrongData.getBytes(StandardCharsets.UTF_8),
                arr.length,
                int[].class,
                DataType.INT8_VECTOR),
        "Invalid I8 number format at index 2: 128");

    // binary
    actualArr = VectorDataUtils.parseBinary(arr, 3, byte[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualArr, arr);

    actualShortArr = VectorDataUtils.parseBinary(arr, 3, short[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualShortArr, shortArr);

    actualIntArr = VectorDataUtils.parseBinary(arr, 3, int[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualIntArr, intArr);

    actualLongArr = VectorDataUtils.parseBinary(arr, 3, long[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualLongArr, longArr);

    actualDoubleArr = VectorDataUtils.parseBinary(arr, 3, double[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    actualFloatArr = VectorDataUtils.parseBinary(arr, 3, float[].class, DataType.INT8_VECTOR);
    assertArrayEquals(actualFloatArr, floatArr);
  }

  @Test
  public void testInt16() {
    short[] arr = new short[] {Short.MIN_VALUE, 22, Short.MAX_VALUE};
    int[] intArr = new int[] {Short.MIN_VALUE, 22, Short.MAX_VALUE};
    long[] longArr = new long[] {Short.MIN_VALUE, 22, Short.MAX_VALUE};
    double[] doubleArr = new double[] {Short.MIN_VALUE, 22, Short.MAX_VALUE};
    float[] floatArr = new float[] {Short.MIN_VALUE, 22, Short.MAX_VALUE};

    String data = Arrays.toString(arr);
    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                byte[].class,
                DataType.INT16_VECTOR),
        "Unable to convert Vector of I16 elements to byte array.");

    short[] actualShortArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            short[].class,
            DataType.INT16_VECTOR);
    assertArrayEquals(actualShortArr, arr);

    int[] actualIntArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, int[].class, DataType.INT16_VECTOR);
    assertArrayEquals(actualIntArr, intArr);

    long[] actualLongArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, long[].class, DataType.INT16_VECTOR);
    assertArrayEquals(actualLongArr, longArr);

    double[] actualDoubleArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            double[].class,
            DataType.INT16_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    float[] actualFloatArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            float[].class,
            DataType.INT16_VECTOR);
    assertArrayEquals(actualFloatArr, floatArr);

    // wrong value
    int[] wrongIntArr = new int[] {-1, 22, Short.MAX_VALUE + 1};
    String wrongData = Arrays.toString(wrongIntArr);
    Common.assertThrowsContains(
        IllegalArgumentException.class,
        () ->
            VectorDataUtils.parse(
                wrongData.getBytes(StandardCharsets.UTF_8),
                arr.length,
                int[].class,
                DataType.INT16_VECTOR),
        "Invalid I16 number format at index 2: 32768");

    // binary
    byte[] binary = encodeShortArray(arr);
    actualShortArr =
        VectorDataUtils.parseBinary(binary, arr.length, short[].class, DataType.INT16_VECTOR);
    assertArrayEquals(actualShortArr, arr);

    actualIntArr =
        VectorDataUtils.parseBinary(binary, arr.length, int[].class, DataType.INT16_VECTOR);
    assertArrayEquals(actualIntArr, intArr);

    actualLongArr =
        VectorDataUtils.parseBinary(binary, arr.length, long[].class, DataType.INT16_VECTOR);
    assertArrayEquals(actualLongArr, longArr);

    actualDoubleArr =
        VectorDataUtils.parseBinary(binary, arr.length, double[].class, DataType.INT16_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    actualFloatArr =
        VectorDataUtils.parseBinary(binary, arr.length, float[].class, DataType.INT16_VECTOR);
    assertArrayEquals(actualFloatArr, floatArr);
  }

  @Test
  public void testInt32() {
    int[] arr = new int[] {Integer.MIN_VALUE, 22, Integer.MAX_VALUE};
    long[] longArr = new long[] {Integer.MIN_VALUE, 22, Integer.MAX_VALUE};
    double[] doubleArr = new double[] {Integer.MIN_VALUE, 22, Integer.MAX_VALUE};
    float[] floatArr = new float[] {Integer.MIN_VALUE, 22, Integer.MAX_VALUE};

    String data = Arrays.toString(arr);
    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                byte[].class,
                DataType.INT32_VECTOR),
        "Unable to convert Vector of I32 elements to byte array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                short[].class,
                DataType.INT32_VECTOR),
        "Unable to convert Vector of I32 elements to short array.");

    int[] actualIntArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, int[].class, DataType.INT32_VECTOR);
    assertArrayEquals(actualIntArr, arr);

    long[] actualLongArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, long[].class, DataType.INT32_VECTOR);
    assertArrayEquals(actualLongArr, longArr);

    double[] actualDoubleArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            double[].class,
            DataType.INT32_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    float[] actualFloatArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            float[].class,
            DataType.INT32_VECTOR);
    assertArrayEquals(actualFloatArr, floatArr);

    // wrong value
    long[] wrongIntArr = new long[] {-1, 22, Integer.toUnsignedLong(Integer.MAX_VALUE) + 1};
    String wrongData = Arrays.toString(wrongIntArr);
    Common.assertThrowsContains(
        IllegalArgumentException.class,
        () ->
            VectorDataUtils.parse(
                wrongData.getBytes(StandardCharsets.UTF_8),
                arr.length,
                int[].class,
                DataType.INT32_VECTOR),
        "Invalid I32 number format at index 2: 2147483648");

    // binary
    byte[] binary = encodeIntArray(arr);
    actualIntArr =
        VectorDataUtils.parseBinary(binary, arr.length, int[].class, DataType.INT32_VECTOR);
    assertArrayEquals(actualIntArr, arr);

    actualLongArr =
        VectorDataUtils.parseBinary(binary, arr.length, long[].class, DataType.INT32_VECTOR);
    assertArrayEquals(actualLongArr, longArr);

    actualDoubleArr =
        VectorDataUtils.parseBinary(binary, arr.length, double[].class, DataType.INT32_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    actualFloatArr =
        VectorDataUtils.parseBinary(binary, arr.length, float[].class, DataType.INT32_VECTOR);
    assertArrayEquals(actualFloatArr, floatArr);
  }

  @Test
  public void testInt64() {
    long[] arr = new long[] {Long.MIN_VALUE, 22, 1243, Long.MAX_VALUE};
    double[] doubleArr = new double[] {Long.MIN_VALUE, 22, 1243, Long.MAX_VALUE};

    String data = Arrays.toString(arr);
    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                byte[].class,
                DataType.INT64_VECTOR),
        "Unable to convert Vector of I64 elements to byte array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                short[].class,
                DataType.INT64_VECTOR),
        "Unable to convert Vector of I64 elements to short array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                int[].class,
                DataType.INT64_VECTOR),
        "Unable to convert Vector of I64 elements to int array.");

    long[] actualLongArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8), arr.length, long[].class, DataType.INT64_VECTOR);
    assertArrayEquals(actualLongArr, arr);

    double[] actualDoubleArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            double[].class,
            DataType.INT64_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    // binary
    byte[] binary = encodeLongArray(arr);
    actualLongArr =
        VectorDataUtils.parseBinary(binary, arr.length, long[].class, DataType.INT64_VECTOR);
    assertArrayEquals(actualLongArr, arr);

    actualDoubleArr =
        VectorDataUtils.parseBinary(binary, arr.length, double[].class, DataType.INT64_VECTOR);
    assertArrayEquals(actualDoubleArr, doubleArr);

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                float[].class,
                DataType.INT64_VECTOR),
        "Unable to convert Vector of I64 elements to float array.");
    // wrong value
    String wrongData = "[-1, 22, 1243, 9223372036854775808]";
    Common.assertThrowsContains(
        IllegalArgumentException.class,
        () ->
            VectorDataUtils.parse(
                wrongData.getBytes(StandardCharsets.UTF_8), 4, long[].class, DataType.INT64_VECTOR),
        "Invalid I64 number format at index 3: 9223372036854775808");
  }

  @Test
  public void testFloat32() {
    float[] arr = new float[] {Float.MIN_VALUE, 0.2672612f, Float.MAX_VALUE};
    double[] expectedDoubleArr =
        new double[] {(double) Float.MIN_VALUE, 0.26726120710372925d, (double) Float.MAX_VALUE};

    String data = Arrays.toString(arr);
    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                byte[].class,
                DataType.FLOAT32_VECTOR),
        "Unable to convert Vector of F32 elements to byte array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                short[].class,
                DataType.FLOAT32_VECTOR),
        "Unable to convert Vector of F32 elements to short array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                int[].class,
                DataType.FLOAT32_VECTOR),
        "Unable to convert Vector of F32 elements to int array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                long[].class,
                DataType.FLOAT32_VECTOR),
        "Unable to convert Vector of F32 elements to long array.");

    double[] actualDoubleArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            double[].class,
            DataType.FLOAT32_VECTOR);
    assertArrayEquals(expectedDoubleArr, actualDoubleArr);

    float[] actualFloatArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            float[].class,
            DataType.FLOAT32_VECTOR);
    assertArrayEquals(arr, actualFloatArr);

    // binary
    byte[] binary = encodeFloatArray(arr);
    actualDoubleArr =
        VectorDataUtils.parseBinary(binary, arr.length, double[].class, DataType.FLOAT32_VECTOR);
    assertArrayEquals(expectedDoubleArr, actualDoubleArr);

    actualFloatArr =
        VectorDataUtils.parseBinary(binary, arr.length, float[].class, DataType.FLOAT32_VECTOR);
    assertArrayEquals(arr, actualFloatArr);

    // wrong value
    String wrongData = "[-143243, 22.2332f, 1243.33, wrong]";
    Common.assertThrowsContains(
        IllegalArgumentException.class,
        () ->
            VectorDataUtils.parse(
                wrongData.getBytes(StandardCharsets.UTF_8),
                4,
                float[].class,
                DataType.FLOAT32_VECTOR),
        "Invalid F32 number format at index 3: wrong");
  }

  @Test
  public void testFloat64() {
    double[] arr = new double[] {Double.MIN_VALUE, 22.219999313354492d, Double.MAX_VALUE};

    String data = Arrays.toString(arr);
    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                byte[].class,
                DataType.FLOAT64_VECTOR),
        "Unable to convert Vector of F64 elements to byte array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                short[].class,
                DataType.FLOAT64_VECTOR),
        "Unable to convert Vector of F64 elements to short array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                int[].class,
                DataType.FLOAT64_VECTOR),
        "Unable to convert Vector of F64 elements to int array.");

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                long[].class,
                DataType.FLOAT64_VECTOR),
        "Unable to convert Vector of F64 elements to long array.");

    double[] actualDoubleArr =
        VectorDataUtils.parse(
            data.getBytes(StandardCharsets.UTF_8),
            arr.length,
            double[].class,
            DataType.FLOAT64_VECTOR);
    assertArrayEquals(actualDoubleArr, arr);

    // binary
    byte[] binary = encodeDoubleArray(arr);
    actualDoubleArr =
        VectorDataUtils.parseBinary(binary, arr.length, double[].class, DataType.FLOAT64_VECTOR);
    assertArrayEquals(actualDoubleArr, arr);

    Common.assertThrowsContains(
        UnsupportedOperationException.class,
        () ->
            VectorDataUtils.parse(
                data.getBytes(StandardCharsets.UTF_8),
                arr.length,
                float[].class,
                DataType.FLOAT64_VECTOR),
        "Unable to convert Vector of F64 elements to float array.");

    // wrong value
    String wrongData = "[-143243, 22.2332f, 1243.33, wrong]";
    Common.assertThrowsContains(
        IllegalArgumentException.class,
        () ->
            VectorDataUtils.parse(
                wrongData.getBytes(StandardCharsets.UTF_8),
                4,
                double[].class,
                DataType.FLOAT64_VECTOR),
        "Invalid F64 number format at index 3: wrong");
  }

  public static byte[] encodeShortArray(short[] shortArray) {
    ByteBuffer buffer = ByteBuffer.allocate(shortArray.length * 2); // Each short is 2 bytes
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (short value : shortArray) {
      buffer.putShort(value);
    }
    return buffer.array();
  }

  public static byte[] encodeIntArray(int[] intArray) {
    ByteBuffer buffer = ByteBuffer.allocate(intArray.length * 4);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int value : intArray) {
      buffer.putInt(value);
    }
    return buffer.array();
  }

  public static byte[] encodeLongArray(long[] longArray) {
    ByteBuffer buffer = ByteBuffer.allocate(longArray.length * 8); // Each long is 8 bytes
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (long value : longArray) {
      buffer.putLong(value);
    }
    return buffer.array();
  }

  public static byte[] encodeFloatArray(float[] floatArray) {
    ByteBuffer buffer = ByteBuffer.allocate(floatArray.length * 4); // Each float is 4 bytes
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (float value : floatArray) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  public static byte[] encodeDoubleArray(double[] doubleArray) {
    ByteBuffer buffer = ByteBuffer.allocate(doubleArray.length * 8); // Each double is 8 bytes
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (double value : doubleArray) {
      buffer.putDouble(value);
    }
    return buffer.array();
  }
}
