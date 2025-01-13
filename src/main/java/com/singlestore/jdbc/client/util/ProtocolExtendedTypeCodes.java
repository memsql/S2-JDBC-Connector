// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.util;

import java.util.Arrays;

public enum ProtocolExtendedTypeCodes {
  NONE(0),
  BSON(1),
  VECTOR(2);
  private final int code;

  ProtocolExtendedTypeCodes(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static ProtocolExtendedTypeCodes fromCode(int code) {
    return Arrays.stream(values())
        .filter(v -> v.getCode() == code)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Wrong extended data type: " + code));
  }
}
