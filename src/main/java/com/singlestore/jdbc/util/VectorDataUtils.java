package com.singlestore.jdbc.util;

import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.util.vector.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class VectorDataUtils {
  private VectorDataUtils() {}

  public static <T> T parse(byte[] data, int length, Class<T> parsedClass, DataType dataType) {
    return (T) getVectorParser(dataType).parse(data, length, parsedClass);
  }

  public static <T> T parseBinary(
      byte[] data, int length, Class<T> parsedClass, DataType dataType) {
    return (T) getBinaryVectorParser(dataType).parse(data, length, parsedClass);
  }

  private static VectorParser getBinaryVectorParser(DataType dataType) {
    switch (dataType) {
      case FLOAT64_VECTOR:
        return Float64VectorBinaryParser.INSTANCE;
      case FLOAT32_VECTOR:
        return Float32VectorBinaryParser.INSTANCE;
      case INT64_VECTOR:
        return Int64VectorBinaryParser.INSTANCE;
      case INT32_VECTOR:
        return Int32VectorBinaryParser.INSTANCE;
      case INT16_VECTOR:
        return Int16VectorBinaryParser.INSTANCE;
      case INT8_VECTOR:
        return Int8VectorBinaryParser.INSTANCE;
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + dataType.name());
    }
  }

  private static VectorParser getVectorParser(DataType dataType) {
    switch (dataType) {
      case FLOAT64_VECTOR:
        return Float64VectorParser.INSTANCE;
      case FLOAT32_VECTOR:
        return Float32VectorParser.INSTANCE;
      case INT64_VECTOR:
        return Int64VectorParser.INSTANCE;
      case INT32_VECTOR:
        return Int32VectorParser.INSTANCE;
      case INT16_VECTOR:
        return Int16VectorParser.INSTANCE;
      case INT8_VECTOR:
        return Int8VectorParser.INSTANCE;
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + dataType.name());
    }
  }
}
