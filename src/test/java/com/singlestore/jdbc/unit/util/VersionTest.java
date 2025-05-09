// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.util.Version;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
public class VersionTest {

  @Test
  public void testValue() {
    Version v = new Version("3.0.0-alpha-SNAPSHOT");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(0, v.getPatchVersion());
    assertEquals("-alpha-SNAPSHOT", v.getQualifier());

    v = new Version("3.0.0=alpha-SNAPSHOT");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(0, v.getPatchVersion());
    assertEquals("=alpha-SNAPSHOT", v.getQualifier());

    v = new Version("3.0.1");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(1, v.getPatchVersion());
    assertEquals("", v.getQualifier());
  }
}
