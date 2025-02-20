// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_100_cols extends Common {

  @Benchmark
  public int[] text(MyState state) throws Throwable {
    return run(state.connectionText);
  }

  @Benchmark
  public int[] binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  @Benchmark
  public int[] binaryNoCache(MyState state) throws Throwable {
    return run(state.connectionBinaryNoCache);
  }

  private int[] run(Connection con) throws Throwable {

    try (PreparedStatement prep = con.prepareStatement("select * FROM test100")) {
      ResultSet rs = prep.executeQuery();
      rs.next();
      int[] objs = new int[100];
      for (int i = 0; i < 100; i++) {
        objs[i] = rs.getInt(i + 1);
      }
      return objs;
    }
  }

}
