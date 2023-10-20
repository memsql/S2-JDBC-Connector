// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_1_user extends Common {

  @Benchmark
  public Object[] text(MyState state) throws Throwable {
    return run(state.connectionText);
  }

  @Benchmark
  public Object[] binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  private Object[] run(Connection con) throws Throwable {
    final int numberOfUserCol = 13;
    try (PreparedStatement prep = con.prepareStatement(
        "select * FROM information_schema.users LIMIT 1")) {
      ResultSet rs = prep.executeQuery();
      rs.next();
      Object[] objs = new Object[numberOfUserCol];
      for (int i = 0; i < numberOfUserCol; i++) {
        objs[i] = rs.getObject(i + 1);
      }
      return objs;
    }
  }
}
