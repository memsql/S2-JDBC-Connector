// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.singlestore.jdbc.codec.Parameter;
import com.singlestore.jdbc.plugin.codec.StringCodec;
import com.singlestore.jdbc.util.ParameterList;
import org.junit.jupiter.api.Test;

public class ParameterListTest {

  @Test
  public void sizeLimit() {
    ParameterList p = new ParameterList();
    p.set(2, new Parameter<>(StringCodec.INSTANCE, "test2"));
    assertNull(p.get(1));
    assertNotNull(p.get(2));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> p.get(3));
  }
}
