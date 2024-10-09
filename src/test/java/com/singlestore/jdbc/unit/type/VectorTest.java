package com.singlestore.jdbc.unit.type;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.type.Vector;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class VectorTest {

  @Test
  public void ofInt8ValuesTest() {
    byte[] arr = new byte[] {0, Byte.MIN_VALUE, Byte.MAX_VALUE, 21};
    String value = "[0,-128,127,21]";

    Vector vector = Vector.ofInt8Values(arr);
    assertEquals(value, vector.stringValue());
    assertEquals(DataType.INT8_VECTOR, vector.getType());
    assertFalse(vector.isBinary());
  }

  @Test
  public void ofInt16ValuesTest() {
    short[] arr = new short[] {0, Short.MIN_VALUE, Short.MAX_VALUE, 127, -75};
    String value = "[0,-32768,32767,127,-75]";
    Vector vector =
        Vector.fromData(value.getBytes(StandardCharsets.UTF_8), 5, DataType.INT16_VECTOR, false);
    assertArrayEquals(arr, vector.toShortArray());

    vector = Vector.ofInt16Values(arr);
    assertEquals(value, vector.stringValue());
    assertEquals(DataType.INT16_VECTOR, vector.getType());
    assertFalse(vector.isBinary());
  }

  @Test
  public void ofInt32ValuesTest() {
    int[] arr = new int[] {0, Integer.MIN_VALUE, Integer.MAX_VALUE, 432, -23445};
    String value = "[0,-2147483648,2147483647,432,-23445]";
    Vector vector =
        Vector.fromData(value.getBytes(StandardCharsets.UTF_8), 5, DataType.INT32_VECTOR, false);
    assertArrayEquals(arr, vector.toIntArray());

    vector = Vector.ofInt32Values(arr);
    assertEquals(value, vector.stringValue());
    assertEquals(DataType.INT32_VECTOR, vector.getType());
    assertFalse(vector.isBinary());
  }

  @Test
  public void ofInt64ValuesTest() {
    long[] arr = new long[] {0, Long.MIN_VALUE, Long.MAX_VALUE, 432221112L, -234432445L};
    String value = "[0,-9223372036854775808,9223372036854775807,432221112,-234432445]";
    Vector vector =
        Vector.fromData(value.getBytes(StandardCharsets.UTF_8), 5, DataType.INT64_VECTOR, false);
    assertArrayEquals(arr, vector.toLongArray());

    vector = Vector.ofInt64Values(arr);
    assertEquals(value, vector.stringValue());
    assertEquals(DataType.INT64_VECTOR, vector.getType());
    assertFalse(vector.isBinary());
  }

  @Test
  public void ofFloat32ValuesTest() {
    float[] arr = new float[] {0f, Float.MIN_VALUE, Float.MAX_VALUE, 432.224f, -23445.313401f};
    String value = "[0.0,1.4E-45,3.4028235E38,432.224,-23445.312]";
    Vector vector =
        Vector.fromData(value.getBytes(StandardCharsets.UTF_8), 5, DataType.FLOAT32_VECTOR, false);
    assertArrayEquals(arr, vector.toFloatArray());

    vector = Vector.ofFloat32Values(arr);
    assertEquals(value, vector.stringValue());
    assertEquals(DataType.FLOAT32_VECTOR, vector.getType());
    assertFalse(vector.isBinary());
  }

  @Test
  public void ofFloat64ValuesTest() {
    double[] arr =
        new double[] {0, Double.MIN_VALUE, Double.MAX_VALUE, 123.34332321012d, -23.33442342341221d};
    String value = "[0.0,4.9E-324,1.7976931348623157E308,123.34332321012,-23.33442342341221]";
    Vector vector =
        Vector.fromData(value.getBytes(StandardCharsets.UTF_8), 5, DataType.FLOAT64_VECTOR, false);
    assertArrayEquals(arr, vector.toDoubleArray());

    vector = Vector.ofFloat64Values(arr);
    assertEquals(value, vector.stringValue());
    assertEquals(DataType.FLOAT64_VECTOR, vector.getType());
    assertFalse(vector.isBinary());
  }
}
