// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.util;

import com.singlestore.jdbc.client.DataType;
import java.util.Arrays;

public enum VectorType {
  NONE(0, DataType.NULL),
  F32(1, DataType.FLOAT32_VECTOR),
  F64(2, DataType.FLOAT64_VECTOR),
  I8(3, DataType.INT8_VECTOR),
  I16(4, DataType.INT16_VECTOR),
  I32(5, DataType.INT32_VECTOR),
  I64(6, DataType.INT64_VECTOR);

  private final int code;
  private final DataType type;

  VectorType(int code, DataType type) {
    this.code = code;
    this.type = type;
  }

  public int getCode() {
    return code;
  }

  public DataType getType() {
    return type;
  }

  public static VectorType fromCode(int code) {
    return Arrays.stream(values())
        .filter(v -> v.getCode() == code)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Wrong extended vector type: " + code));
  }
}
