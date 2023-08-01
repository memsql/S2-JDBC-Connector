// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import java.sql.*;
import org.junit.jupiter.api.*;

public class CompressTest extends Common {
  private static Connection shareCompressCon;

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS compressTest");
    stmt.execute(
        "CREATE TABLE compressTest (t1 int not null primary key auto_increment, "
            + "t2 LONGTEXT, t3 LONGTEXT, t4 LONGTEXT, t5 LONGTEXT, t6 LONGTEXT)");
    shareCompressCon = createCon("useCompression=true");
  }

  @AfterAll
  public static void drop() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IF EXISTS compressTest");
    shareCompressCon.close();
  }

  @Test
  public void bigSend() throws SQLException {
    bigSend(sharedConn);
    bigSend(sharedConnBinary);
    bigSend(shareCompressCon);
  }

  public void bigSend(Connection con) throws SQLException {
    char[] arr2 = new char[Math.min(16 * 1024 * 1024, getMaxAllowedPacket(sharedConn) / 2)];
    for (int pos = 0; pos < arr2.length; pos++) {
      arr2[pos] = (char) ('A' + (pos % 60));
    }
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE compressTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    String st2 = new String(arr2, 0, 5000);
    String st3 = new String(arr2, 0, 150000);
    String st4 = new String(arr2, 0, 1100000);
    String st5 = new String(arr2, 0, 1100000);
    String st6 = new String(arr2);
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO compressTest VALUES (?, ?, ?, ?, ?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, st2);
      prep.setString(3, st3);
      prep.setString(4, st4);
      prep.setString(5, st5);
      prep.setString(6, st6);
      prep.execute();

      prep.setInt(1, 2);
      prep.setString(2, st2);
      prep.setString(3, st3);
      prep.setString(4, st4);
      prep.setString(5, st5);
      prep.setString(6, st6);
      prep.execute();
    }
    stmt.setFetchSize(1);
    ResultSet rs = stmt.executeQuery("SELECT * from compressTest");
    assertTrue(rs.next());

    assertEquals(st2, rs.getString(2));
    assertEquals(st3, rs.getString(3));
    assertEquals(st4, rs.getString(4));
    assertEquals(st5, rs.getString(5));
    assertEquals(st6, rs.getString(6));
    rs.close();
    stmt.execute("COMMIT");
  }
}
