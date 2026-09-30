// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.*;

public class DriverTest extends Common {

  /** Resource path must stay package-scoped so generic classpath scanners skip it. */
  private static final String DRIVER_PROPERTIES = "com/singlestore/jdbc/driver.properties";

  @Test
  public void ensureDescriptionFilled() throws IOException, NoSuchFieldException {
    Properties descr = new Properties();
    try (InputStream inputStream =
        Common.class.getClassLoader().getResourceAsStream(DRIVER_PROPERTIES)) {
      assertNotNull(inputStream, "missing " + DRIVER_PROPERTIES);
      descr.load(inputStream);
    }

    // root-level driver.properties must not be packaged (TIBCO DataSynapse / Zendesk 54638)
    assertNull(
        Common.class.getClassLoader().getResource("driver.properties"),
        "driver.properties must not be at JAR/classpath root");

    // check that description is present
    for (Field field : Configuration.Builder.class.getDeclaredFields()) {
      if (!field.getName().startsWith("_")) {
        if (descr.get(field.getName()) == null && !"$jacocoData".equals(field.getName()))
          throw new IllegalStateException(String.format("Missing %s description", field.getName()));
      }
    }

    // check that no description without option, and values avoid XML/SGML-restricted chars
    for (Map.Entry<Object, Object> entry : descr.entrySet()) {
      // NoSuchFieldException will be thrown if not present
      Configuration.Builder.class.getDeclaredField(entry.getKey().toString());
      String value = entry.getValue().toString();
      assertFalse(
          value.chars().anyMatch(c -> c == '<' || c == '>' || c == '"' || c == '&'),
          () ->
              String.format(
                  "Property: %s , value: %s cannot contain any of these characters: <>\"&",
                  entry.getKey(), value));
    }
  }

  @Test
  public void getPropertyInfo() throws SQLException {
    Driver driver = new com.singlestore.jdbc.Driver();
    assertEquals(0, driver.getPropertyInfo(null, null).length);
    assertEquals(0, driver.getPropertyInfo("jdbc:bla//", null).length);

    Properties properties = new Properties();
    properties.put("password", "myPwd");
    DriverPropertyInfo[] driverPropertyInfos =
        driver.getPropertyInfo("jdbc:singlestore://localhost/db?user=root", properties);
    for (DriverPropertyInfo driverPropertyInfo : driverPropertyInfos) {
      if (!"$jacocoData".equals(driverPropertyInfo.name)) {
        assertNotNull(
            driverPropertyInfo.description, "no description for " + driverPropertyInfo.name);
      }
    }
  }

  @Test
  public void basicInfo() {
    Driver driver = new com.singlestore.jdbc.Driver();
    assertEquals(1, driver.getMajorVersion());
    assertTrue(driver.getMinorVersion() > -1);
    assertTrue(driver.jdbcCompliant());
    assertThrows(SQLFeatureNotSupportedException.class, () -> driver.getParentLogger());
  }
}
