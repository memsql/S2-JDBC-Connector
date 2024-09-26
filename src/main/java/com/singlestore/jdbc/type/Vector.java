package com.singlestore.jdbc.type;

import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.util.VectorDataUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents an ordered collection of numeric values with a fixed number of dimensions (length).
 *
 * <p>Supported element types:
 *
 * <ul>
 *   <li>{@link DataType#INT8_VECTOR}INT8_VECTOR (8-bit signed integer)
 *   <li>{@link DataType#INT16_VECTOR} (16-bit signed integer)
 *   <li>{@link DataType#INT32_VECTOR} (32-bit signed integer)
 *   <li>{@link DataType#INT64_VECTOR} (64-bit signed integer)
 *   <li>{@link DataType#FLOAT32_VECTOR} (32-bit floating-point number, default)
 *   <li>{@link DataType#FLOAT64_VECTOR} (64-bit floating-point number)
 * </ul>
 *
 * <p>For further details, see the <a
 * href="https://docs.singlestore.com/cloud/reference/sql-reference/data-types/vector-type/">
 * SingleStore Vector Type</a>.
 */
public class Vector {

  private final byte[] values;
  private final int length;
  private final DataType type;
  private final boolean isBinary;

  private Vector(byte[] values, int length, DataType type, boolean isBinary) {
    this.values = values;
    this.length = length;
    this.type = type;
    this.isBinary = isBinary;
  }

  public static Vector fromData(byte[] values, int length, DataType dataType, boolean isBinary) {
    return new Vector(values, length, dataType, isBinary);
  }

  public static Vector ofFloat64Values(double[] values) {
    String data = Arrays.toString(values).replace(" ", "");
    return fromData(
        data.getBytes(StandardCharsets.UTF_8), values.length, DataType.FLOAT64_VECTOR, false);
  }

  public static Vector ofFloat32Values(float[] values) {
    String data = Arrays.toString(values).replace(" ", "");
    return fromData(
        data.getBytes(StandardCharsets.UTF_8), values.length, DataType.FLOAT32_VECTOR, false);
  }

  public static Vector ofInt8Values(byte[] values) {
    String data = Arrays.toString(values).replace(" ", "");
    return fromData(
        data.getBytes(StandardCharsets.UTF_8), values.length, DataType.INT8_VECTOR, false);
  }

  public static Vector ofInt16Values(short[] values) {
    String data = Arrays.toString(values).replace(" ", "");
    return fromData(
        data.getBytes(StandardCharsets.UTF_8), values.length, DataType.INT16_VECTOR, false);
  }

  public static Vector ofInt32Values(int[] values) {
    String data = Arrays.toString(values).replace(" ", "");
    return fromData(
        data.getBytes(StandardCharsets.UTF_8), values.length, DataType.INT32_VECTOR, false);
  }

  public static Vector ofInt64Values(long[] values) {
    String data = Arrays.toString(values).replace(" ", "");
    return fromData(
        data.getBytes(StandardCharsets.UTF_8), values.length, DataType.INT64_VECTOR, false);
  }

  /** Get byte array of Vector value. */
  public byte[] getValues() {
    return Arrays.copyOf(values, values.length);
  }

  public boolean isBinary() {
    return isBinary;
  }

  /** Get Vector type. */
  public DataType getType() {
    return type;
  }

  public String stringValue() {
    return new String(values);
  }

  public String[] toStringArray() {
    if (!isBinary()) {
      return VectorDataUtils.parseVectorString(values, length);
    }
    throw new IllegalStateException("Cannot convert vector in binary format to String array.");
  }

  public float[] toFloatArray() {
    if (!isBinary()) {
      return VectorDataUtils.parse(values, length, float[].class, type);
    }
    throw new IllegalStateException("Cannot convert vector in binary format to float array.");
  }

  public double[] toDoubleArray() {
    if (!isBinary()) {
      return VectorDataUtils.parse(values, length, double[].class, type);
    }
    throw new IllegalStateException("Cannot convert vector in binary format to double array.");
  }

  public int[] toIntArray() {
    if (!isBinary()) {
      return VectorDataUtils.parse(values, length, int[].class, type);
    }
    throw new IllegalStateException("Cannot convert vector in binary format to int array.");
  }

  public long[] toLongArray() {
    if (!isBinary()) {
      return VectorDataUtils.parse(values, length, long[].class, type);
    }
    throw new IllegalStateException("Cannot convert vector in binary format to long array.");
  }

  public short[] toShortArray() {
    if (!isBinary()) {
      return VectorDataUtils.parse(values, length, short[].class, type);
    }
    throw new IllegalStateException("Cannot convert vector in binary format to short array.");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Vector vector = (Vector) o;
    return length == vector.length
        && isBinary == vector.isBinary
        && Objects.deepEquals(values, vector.values)
        && type == vector.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(values), length, type, isBinary);
  }

  @Override
  public String toString() {
    return "Vector{"
        + "values="
        + stringValue()
        + ", length="
        + length
        + ", type="
        + type
        + ", isBinary="
        + isBinary
        + '}';
  }
}
