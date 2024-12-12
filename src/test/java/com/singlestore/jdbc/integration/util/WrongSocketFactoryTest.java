// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.integration.util;

import com.singlestore.jdbc.integration.ConnectionTest;

public class WrongSocketFactoryTest {

  static {
    ConnectionTest.staticTestValue = 50;
  }

  public WrongSocketFactoryTest() {
    ConnectionTest.staticTestValue = 100;
  }
}
