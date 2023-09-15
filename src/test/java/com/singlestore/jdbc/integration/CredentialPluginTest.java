// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CredentialPluginTest extends Common {

  /**
   * Create temporary test User.
   *
   * @throws SQLException if any
   */
  @BeforeAll
  public static void beforeTest() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "GRANT SELECT ON "
            + sharedConn.getCatalog()
            + ".* TO 'identityUser'@'localhost' IDENTIFIED BY '!Passw0rd3Works'");
    stmt.execute(
        "GRANT SELECT ON "
            + sharedConn.getCatalog()
            + ".* TO 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
    stmt.execute("FLUSH PRIVILEGES");
  }

  /**
   * remove temporary test User.
   *
   * @throws SQLException if any
   */
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("DROP USER 'identityUser'@'%'");
    } catch (SQLException e) {
      // eat
    }
    try {
      stmt.execute("DROP USER 'identityUser'@'localhost'");
    } catch (SQLException e) {
      // eat
    }
  }

  @Test
  public void propertiesIdentityTest() throws SQLException {
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("credentialType=PROPERTY&user=identityUser"),
        "Access denied");

    System.setProperty("singlestore.user", "identityUser");
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("credentialType=PROPERTY&pwdKey=myPwdKey"),
        "Access denied");

    System.setProperty("myPwdKey", "!Passw0rd3Works");
    try (Connection conn = createCon("credentialType=PROPERTY&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }

    System.setProperty("singlestore.pwd", "!Passw0rd3Works");

    try (Connection conn = createCon("credentialType=PROPERTY")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void specificPropertiesIdentityTest() throws SQLException {

    System.setProperty("myUserKey", "identityUser");
    System.setProperty("myPwdKey", "!Passw0rd3Works");

    try (Connection conn = createCon("credentialType=PROPERTY&userKey=myUserKey&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  public void unknownCredentialTest() {
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("credentialType=UNKNOWN"),
        "No identity plugin registered with the type \"UNKNOWN\"");
  }

  @Test
  public void envsIdentityTest() throws Exception {
    Map<String, String> tmpEnv = new HashMap<>();

    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("&user=toto&credentialType=ENV&pwdKey=myPwdKey"),
        "Access denied");

    tmpEnv.put("myPwdKey", "!Passw0rd3Works");
    setEnv(tmpEnv);

    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon("&user=toto&credentialType=ENV&pwdKey=myPwdKey"),
        "Access denied");

    try (Connection conn = createCon("user=identityUser&credentialType=ENV&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }

    tmpEnv.put("SINGLESTORE_USER", "identityUser");
    setEnv(tmpEnv);

    try (Connection conn = createCon("credentialType=ENV&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }

    tmpEnv.put("SINGLESTORE_PWD", "!Passw0rd3Works");
    setEnv(tmpEnv);

    try (Connection conn = createCon("credentialType=ENV")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }

    tmpEnv = new HashMap<>();
    tmpEnv.put("myUserKey", "identityUser");
    tmpEnv.put("myPwdKey", "!Passw0rd3Works");
    setEnv(tmpEnv);

    try (Connection conn = createCon("credentialType=ENV&userKey=myUserKey&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void envTestsIdentityTest() throws Exception {
    Assumptions.assumeTrue(haveSsl());
    Map<String, String> tmpEnv = new HashMap<>();
    tmpEnv.put("SINGLESTORE2_USER", "identityUser");
    tmpEnv.put("SINGLESTORE2_PWD", "!Passw0rd3Works");
    setEnv(tmpEnv);

    assertThrows(SQLException.class, () -> createCon("credentialType=ENVTEST&sslMode=DISABLE"));
    assertThrows(SQLException.class, () -> createCon("credentialType=ENVTEST"));

    try (Connection conn = createCon("credentialType=ENVTEST&sslMode=TRUST")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT '5'");
      assertTrue(rs.next());
      assertEquals("5", rs.getString(1));
    }
  }

  /**
   * Hack to add env variable for unit testing only
   *
   * @param newenv new env variable
   * @throws Exception if any exception occurs
   */
  @SuppressWarnings("unchecked")
  protected static void setEnv(Map<String, String> newenv) throws Exception {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField =
          processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv =
          (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class<?>[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class<?> cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.putAll(newenv);
        }
      }
    }
  }
}
