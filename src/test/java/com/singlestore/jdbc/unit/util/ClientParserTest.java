// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.util.ClientParser;
import com.singlestore.jdbc.util.RewriteClientParser;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
public class ClientParserTest {

  private void parse(String sql, String[] expected, boolean isRewriteApplicable) {
    ClientParser parser = ClientParser.parameterParts(sql);
    assertEquals(expected.length, parser.getParamCount() + 1, displayErr(parser, expected));

    int pos = 0;
    int paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      assertEquals(expected[i], new String(parser.getQuery(), pos, paramPos - pos));
      pos = paramPos + 1;
    }
    assertEquals(expected[expected.length - 1], new String(parser.getQuery(), pos, paramPos - pos));
    assertEquals(isRewriteApplicable, parser.isRewriteBatchedApplicable());
  }

  private String displayErr(ClientParser parser, String[] exp) {
    StringBuilder sb = new StringBuilder();
    sb.append("is:\n");

    int pos = 0;
    int paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      sb.append(new String(parser.getQuery(), pos, paramPos - pos, StandardCharsets.UTF_8))
          .append("\n");
      pos = paramPos + 1;
    }
    sb.append(new String(parser.getQuery(), pos, paramPos - pos));

    sb.append("but was:\n");
    for (String s : exp) {
      sb.append(s).append("\n");
    }
    return sb.toString();
  }

  @Test
  public void testClientParser() {
    parse(
        "SELECT '\\\\test' /*test* #/ ;`*/",
        new String[] {"SELECT '\\\\test' /*test* #/ ;`*/"},
        false);
    parse("DO '\\\"', \"\\'\"", new String[] {"DO '\\\"', \"\\'\""}, false);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2)",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2)"},
        true);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE KEY UPDATE",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE KEY UPDATE"},
        true);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE"},
        true);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2) ONDUPLICATE",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ONDUPLICATE"},
        true);
    parse(
        "REPLACE INTO TABLE(id,val) VALUES (1,2)",
        new String[] {"REPLACE INTO TABLE(id,val) VALUES (1,2)"},
        true);
  }

  private void rewriteParse(
      String sql, String[] expectedParts, int expectedParamCount, int expectedQueryPartsLength) {
    RewriteClientParser parser = RewriteClientParser.rewritableParts(sql);
    assertEquals(expectedParts.length, parser.getQueryParts().size());
    assertEquals(expectedParamCount, parser.getParamCount());
    assertEquals(expectedQueryPartsLength, parser.getQueryPartsLength());
    for (int i = 0; i < expectedParts.length; i++) {
      assertEquals(expectedParts[i], new String(parser.getQueryParts().get(i)));
    }
  }

  @Test
  public void testClientParserFlag() {
    assertFalse(ClientParser.parameterParts("SELECT * FROM TEST").isRewriteBatchedApplicable());
    assertFalse(
        ClientParser.parameterParts("UPDATE TEST SET t0 = 1 WHERE t2 == 1")
            .isRewriteBatchedApplicable());
    assertFalse(ClientParser.parameterParts("DROP TABLE TEST").isRewriteBatchedApplicable());
    assertFalse(
        ClientParser.parameterParts("DELETE FROM TEST WHERE t2 == 1").isRewriteBatchedApplicable());
    assertFalse(ClientParser.parameterParts("TRUNCATE TABLE TEST").isRewriteBatchedApplicable());
    assertTrue(
        ClientParser.parameterParts("INSERT INTO TEST(t0, t1) VALUES (?, ?)")
            .isRewriteBatchedApplicable());
    assertTrue(
        ClientParser.parameterParts(
                "INSERT INTO TEST(t0, t1) VALUES (?, ?) ON DUPLICATE KEY UPDATE t1 = IF(t1 IS NULL, NOW(6), t1)")
            .isRewriteBatchedApplicable());
    assertTrue(
        ClientParser.parameterParts("INSERT INTO TEST(t0, t1) VALUES (LAST_INSERTED_ID(), NOW(6))")
            .isRewriteBatchedApplicable());
    assertTrue(
        ClientParser.parameterParts("REPLACE INTO TEST(t0, t1) VALUES (?, ?)")
            .isRewriteBatchedApplicable());
    assertTrue(
        ClientParser.parameterParts("REPLACE INTO TEST(t0, t1) VALUES (LAST_INSERTED_ID(), ?)")
            .isRewriteBatchedApplicable());
  }

  @Test
  public void testRewriteClientParser() {
    rewriteParse("SELECT * FROM TEST", new String[] {"SELECT * FROM TEST", "", ""}, 0, 19);
    rewriteParse(
        "INSERT INTO TEST(t0, t1) VALUES (?, ?) ON DUPLICATE KEY UPDATE t1 = IF(t1 IS NULL, NOW(6), t1)",
        new String[] {
          "INSERT INTO TEST(t0, t1) VALUES",
          " (",
          ", ",
          ")",
          " ON DUPLICATE KEY UPDATE t1 = IF(t1 IS NULL, NOW(6), t1)"
        },
        2,
        93);
    rewriteParse(
        "REPLACE INTO TEST(t0, t1, t2, t3, t4, t5) VALUES (?, ?, 'test', ?, 'test2', ?)",
        new String[] {
          "REPLACE INTO TEST(t0, t1, t2, t3, t4, t5) VALUES",
          " (",
          ", ",
          ", 'test', ",
          ", 'test2', ",
          ")",
          ""
        },
        4,
        75);
  }

  @Test
  public void testRewriteClientParserFlag() {
    assertFalse(
        RewriteClientParser.rewritableParts("SELECT * FROM TEST").isQueryMultiValuesRewritable());
    assertFalse(
        RewriteClientParser.rewritableParts("UPDATE TEST SET t0 = 1 WHERE t2 == 1")
            .isQueryMultiValuesRewritable());
    assertFalse(
        RewriteClientParser.rewritableParts("DROP TABLE TEST").isQueryMultiValuesRewritable());
    assertFalse(
        RewriteClientParser.rewritableParts("DELETE FROM TEST WHERE t2 == 1")
            .isQueryMultiValuesRewritable());
    assertFalse(
        RewriteClientParser.rewritableParts("TRUNCATE TABLE TEST").isQueryMultiValuesRewritable());
    assertTrue(
        RewriteClientParser.rewritableParts("INSERT INTO TEST(t0, t1) VALUES (?, ?)")
            .isQueryMultiValuesRewritable());
    assertTrue(
        RewriteClientParser.rewritableParts(
                "INSERT INTO TEST(t0, t1) VALUES (?, ?) ON DUPLICATE KEY UPDATE t1 = IF(t1 IS NULL, NOW(6), t1)")
            .isQueryMultiValuesRewritable());
    assertFalse(
        RewriteClientParser.rewritableParts(
                "INSERT INTO TEST(t0, t1) VALUES (LAST_INSERT_ID(), NOW(6))")
            .isQueryMultiValuesRewritable());
    assertTrue(
        RewriteClientParser.rewritableParts("REPLACE INTO TEST(t0, t1) VALUES (?, ?)")
            .isQueryMultiValuesRewritable());
    assertFalse(
        RewriteClientParser.rewritableParts(
                "REPLACE INTO TEST(t0, t1) VALUES (LAST_INSERT_ID(), ?)")
            .isQueryMultiValuesRewritable());
  }
}
