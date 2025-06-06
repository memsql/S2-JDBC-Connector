// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.singlestore.jdbc.*;
import com.singlestore.jdbc.export.HaMode;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.integration.Common;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
public class ConfigurationTest extends Common {

  @Test
  public void testWrongFormat() {
    assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse("jdbc:singlestore:/localhost/test"),
        "url parsing error : '//' is not present in the url");
  }

  @Test
  public void testParseProps() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test", null);
    assertEquals(0, conf.socketTimeout());

    Properties props = new Properties();
    props.setProperty("socketTimeout", "50");
    conf = Configuration.parse("jdbc:singlestore://localhost/test", props);
    assertEquals(50, conf.socketTimeout());
  }

  @Test
  public void testCredentialType() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test?credentialType=");
    assertNull(conf.credentialPlugin());
  }

  @Test
  public void testWrongHostFormat() {
    assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse("jdbc:singlestore://localhost:wrongPort/test"),
        "Incorrect port value : wrongPort");
  }

  @Test
  public void testNoAdditionalPart() throws SQLException {
    assertEquals(null, Configuration.parse("jdbc:singlestore://localhost/").database());
    assertEquals(
        null, Configuration.parse("jdbc:singlestore://localhost/?socketTimeout=50").user());
    assertEquals(null, Configuration.parse("jdbc:singlestore://localhost").database());
    assertEquals(null, Configuration.parse("jdbc:singlestore://localhost").user());
    assertEquals(
        50,
        Configuration.parse("jdbc:singlestore://localhost?socketTimeout=50&file=/tmp/test")
            .socketTimeout());
    assertEquals(null, Configuration.parse("jdbc:singlestore://localhost?").user());
  }

  @Test
  public void testAliases() throws SQLException {
    assertEquals(
        "someCipher",
        Configuration.parse("jdbc:singlestore://localhost/?enabledSSLCipherSuites=someCipher")
            .enabledSslCipherSuites());
  }

  @Test
  public void testDatabaseOnly() throws SQLException {
    assertEquals("DB", Configuration.parse("jdbc:singlestore://localhost/DB").database());
    assertEquals(null, Configuration.parse("jdbc:singlestore://localhost/DB").user());
  }

  @Test
  void useMysqlMetadata() throws SQLException {
    assertEquals("SingleStore", sharedConn.getMetaData().getDatabaseProductName());
    try (Connection conn = createCon("&useMysqlMetadata=true")) {
      assertEquals("MySQL", conn.getMetaData().getDatabaseProductName());
    }
  }

  @Test
  public void testUrl() throws SQLException {
    Configuration conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306)
            .haMode(HaMode.SEQUENTIAL)
            .build();
    assertEquals("jdbc:singlestore:sequential://local/DB", conf.initialUrl());
    assertEquals("jdbc:singlestore:sequential://local/DB", conf.toString());
    assertEquals(Configuration.parse("jdbc:singlestore:sequential://local/DB"), conf);

    conf =
        new Configuration.Builder()
            .database("DB")
            .addresses(HostAddress.from("local", 3306), HostAddress.from("host2", 3307))
            .haMode(HaMode.SEQUENTIAL)
            .build();

    assertEquals(
        "jdbc:singlestore:sequential://local,address=(host=host2)(port=3307)/DB",
        conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306)
            .haMode(HaMode.SEQUENTIAL)
            .socketTimeout(50)
            .build();
    assertEquals("jdbc:singlestore:sequential://local/DB?socketTimeout=50", conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306)
            .addHost("local", 3307)
            .addHost("local", 3308)
            .haMode(HaMode.LOADBALANCE)
            .socketTimeout(50)
            .build();
    assertEquals(
        "jdbc:singlestore:loadbalance://local,address=(host=local)(port=3307),address=(host=local)(port=3308)/DB?socketTimeout=50",
        conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306)
            .addHost("local", 3307)
            .addHost("local", 3308)
            .haMode(HaMode.LOADBALANCE)
            .socketTimeout(50)
            .build();
    assertEquals(
        "jdbc:singlestore:loadbalance://local,address=(host=local)(port=3307),address=(host=local)(port=3308)/DB?socketTimeout=50",
        conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306)
            .autocommit(false)
            .haMode(HaMode.SEQUENTIAL)
            .build();
    assertEquals("jdbc:singlestore:sequential://local/DB?autocommit=false", conf.initialUrl());
  }

  @Test
  public void testAcceptsUrl() {
    Driver driver = new Driver();
    assertFalse(driver.acceptsURL(null));
    assertTrue(driver.acceptsURL("jdbc:singlestore://localhost/test"));
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost/test"));
  }

  @Test
  public void testConfigurationIsolation() throws Throwable {
    Configuration conf =
        Configuration.parse("jdbc:singlestore://localhost/test?transactionIsolation=readCommitted");
    assertTrue(TransactionIsolation.READ_COMMITTED == conf.transactionIsolation());

    try {
      Configuration.parse("jdbc:singlestore://localhost/test?transactionIsolation=wrong_val");
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains("Wrong argument value 'wrong_val' for TransactionIsolation"));
    }
  }

  @Test
  public void testSslAlias() throws Throwable {
    Configuration conf =
        Configuration.parse("jdbc:singlestore://localhost/test?sslMode=verify-full");
    assertTrue(SslMode.VERIFY_FULL == conf.sslMode());

    conf = Configuration.parse("jdbc:singlestore://localhost/test?sslMode=verify_full");
    assertTrue(SslMode.VERIFY_FULL == conf.sslMode());

    conf = Configuration.parse("jdbc:singlestore://localhost/test?sslMode=trust");
    assertTrue(SslMode.TRUST == conf.sslMode());

    try {
      Configuration.parse("jdbc:singlestore://localhost/test?sslMode=wrong_trust");
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Wrong argument value 'wrong_trust' for SslMode"));
    }

    conf = Configuration.parse("jdbc:singlestore://localhost/test?sslMode=verify-ca");
    assertTrue(SslMode.VERIFY_CA == conf.sslMode());

    conf = Configuration.parse("jdbc:singlestore://localhost/test");
    assertTrue(SslMode.DISABLE == conf.sslMode());

    conf = Configuration.parse("jdbc:singlestore://localhost/test?sslMode");
    assertTrue(SslMode.DISABLE == conf.sslMode());

    conf = Configuration.parse("jdbc:singlestore://localhost/test?sslMode=0");
    assertTrue(SslMode.DISABLE == conf.sslMode());

    conf = Configuration.parse("jdbc:singlestore://localhost/test?sslMode=1");
    assertTrue(SslMode.VERIFY_FULL == conf.sslMode());

    conf = Configuration.parse("jdbc:singlestore://localhost/test?sslMode=true");
    assertTrue(SslMode.VERIFY_FULL == conf.sslMode());
  }

  @Test
  public void testSslCompatibility() throws Throwable {
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:singlestore://localhost/test?useSsl").sslMode());
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:singlestore://localhost/test?useSsl=true").sslMode());
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:singlestore://localhost/test?useSsl=1").sslMode());
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:singlestore://localhost/test?useSSL=1").sslMode());
    assertEquals(
        SslMode.TRUST,
        Configuration.parse("jdbc:singlestore://localhost/test?useSsl&trustServerCertificate")
            .sslMode());
    assertEquals(
        SslMode.VERIFY_CA,
        Configuration.parse(
                "jdbc:singlestore://localhost/test?useSsl&disableSslHostnameVerification")
            .sslMode());
  }

  @Test
  public void testBooleanDefault() throws Throwable {
    assertFalse(
        Configuration.parse("jdbc:singlestore:///test").includeThreadDumpInDeadlockExceptions());
    assertFalse(
        Configuration.parse("jdbc:singlestore:///test?includeThreadDumpInDeadlockExceptions=false")
            .includeThreadDumpInDeadlockExceptions());
    assertTrue(
        Configuration.parse("jdbc:singlestore:///test?includeThreadDumpInDeadlockExceptions=true")
            .includeThreadDumpInDeadlockExceptions());
    assertTrue(
        Configuration.parse("jdbc:singlestore:///test?includeThreadDumpInDeadlockExceptions")
            .includeThreadDumpInDeadlockExceptions());
  }

  @Test
  public void testOptionTakeDefault() throws Throwable {
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test");
    assertEquals(30_000, conf.connectTimeout());
    assertEquals(250, conf.prepStmtCacheSize());
    assertNull(conf.user());
    assertEquals(0, conf.socketTimeout());
    int initialLoginTimeout = DriverManager.getLoginTimeout();
    DriverManager.setLoginTimeout(60);
    conf = Configuration.parse("jdbc:singlestore://localhost/test");
    assertEquals(60_000, conf.connectTimeout());
    DriverManager.setLoginTimeout(initialLoginTimeout);
  }

  @Test
  public void testOptionParse() throws Throwable {
    Configuration conf =
        Configuration.parse(
            "jdbc:singlestore://localhost/test?user=root&password=toto&createDB=true"
                + "&autoReconnect=true&prepStmtCacheSize=2&connectTimeout=5&socketTimeout=20");
    assertEquals(5, conf.connectTimeout());
    assertEquals(20, conf.socketTimeout());
    assertEquals(2, conf.prepStmtCacheSize());
    assertEquals("true", conf.nonMappedOptions().get("createDB"));
    assertEquals("true", conf.nonMappedOptions().get("autoReconnect"));
    assertEquals("root", conf.user());
    assertEquals("toto", conf.password());
  }

  @Test
  public void wrongTypeParsing() {
    assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse("jdbc:singlestore://localhost/test?socketTimeout=20aa"),
        "Optional parameter socketTimeout must be Integer, was '20aa'");
  }

  @Test
  public void testOptionParseSlash() throws Throwable {
    Configuration jdbc =
        Configuration.parse(
            "jdbc:singlestore://127.0.0.1:3306/colleo?user=root&password=toto"
                + "&localSocket=/var/run/mysqld/mysqld.sock");
    assertEquals("/var/run/mysqld/mysqld.sock", jdbc.localSocket());
    assertEquals("root", jdbc.user());
    assertEquals("toto", jdbc.password());
  }

  @Test
  public void testOptionParseIntegerMinimum() throws Throwable {
    Configuration jdbc =
        Configuration.parse(
            "jdbc:singlestore://localhost/test?user=root&autoReconnect=true"
                + "&prepStmtCacheSize=240&connectTimeout=5");
    assertEquals(5, jdbc.connectTimeout());
    assertEquals(240, jdbc.prepStmtCacheSize());
  }

  @Test
  public void testWithoutDb() throws Throwable {
    Configuration jdbc =
        Configuration.parse("jdbc:singlestore://localhost/?user=root&tcpKeepAlive=true");
    assertTrue(jdbc.tcpKeepAlive());
    assertNull(jdbc.database());

    Configuration jdbc2 =
        Configuration.parse("jdbc:singlestore://localhost?user=root&tcpKeepAlive=true");
    assertTrue(jdbc2.tcpKeepAlive());
    assertNull(jdbc2.database());
  }

  @Test
  public void testOptionParseIntegerNotPossible() throws Throwable {
    assertThrows(
        SQLException.class,
        () ->
            Configuration.parse(
                "jdbc:singlestore://localhost/test?user=root&autoReconnect=true&prepStmtCacheSize=-2"
                    + "&connectTimeout=5"));
  }

  @Test()
  public void testJdbcParserSimpleIpv4basic() throws SQLException {
    String url = "jdbc:singlestore://master:3306,child1:3307,child2:3308/database";
    Configuration conf = Configuration.parse(url);
    assertEquals(
        "jdbc:singlestore://master,address=(host=child1)(port=3307),address=(host=child2)(port=3308)/database",
        conf.initialUrl());
    url =
        "jdbc:singlestore://address=(host=master)(port=3305),address=(host=child1)(port=3307),address=(host=child2)(port=3308)/database";
    conf = Configuration.parse(url);
    assertEquals(
        "jdbc:singlestore://master:3305,address=(host=child1)(port=3307),address=(host=child2)(port=3308)/database",
        conf.initialUrl());
    url =
        "jdbc:singlestore://address=(host=master)(port=3306),address=(host=child1)(port=3307),address=(host=child2)(port=3308)/database";
    conf = Configuration.parse(url);
    assertEquals(
        "jdbc:singlestore://master,address=(host=child1)(port=3307),address=(host=child2)(port=3308)/database",
        conf.initialUrl());
    url = "jdbc:singlestore:loadbalance://master:3305,child1:3307,child2:3308/database";
    conf = Configuration.parse(url);
    assertEquals(
        "jdbc:singlestore:loadbalance://master:3305,address=(host=child1)(port=3307),address=(host=child2)(port=3308)/database",
        conf.initialUrl());
  }

  @Test
  public void testJdbcParserSimpleIpv4basicError() throws SQLException {
    Configuration Configuration = com.singlestore.jdbc.Configuration.parse(null);
    assertTrue(Configuration == null);
  }

  @Test
  public void testJdbcParserSimpleIpv4basicwithoutDatabase() throws SQLException {
    String url = "jdbc:singlestore://master:3306,child1:3307,child2:3308/";
    Configuration conf = com.singlestore.jdbc.Configuration.parse(url);
    assertNull(conf.database());
    assertNull(conf.user());
    assertNull(conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("child1", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child2", 3308), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserWithoutDatabaseWithProperties() throws SQLException {
    String url = "jdbc:singlestore://master:3306,child1:3307,child2:3308?autoReconnect=true";
    Configuration conf = com.singlestore.jdbc.Configuration.parse(url);
    assertNull(conf.database());
    assertNull(conf.user());
    assertNull(conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("child1", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child2", 3308), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserSimpleIpv4Properties() throws SQLException {
    String url =
        "jdbc:singlestore://master:3306,child1:3307,child2:3308/database?autoReconnect=true";

    Properties prop = new Properties();
    prop.setProperty("user", "greg");
    prop.setProperty("password", "pass");
    prop.setProperty("allowMultiQueries", "true");

    Configuration conf = com.singlestore.jdbc.Configuration.parse(url, prop);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertTrue(conf.allowMultiQueries());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("child1", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child2", 3308), conf.addresses().get(2));

    prop = new Properties();
    prop.put("user", "greg");
    prop.put("password", "pass");
    prop.put("allowMultiQueries", true);

    conf = com.singlestore.jdbc.Configuration.parse(url, prop);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertTrue(conf.allowMultiQueries());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("child1", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child2", 3308), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserBooleanOption() {
    String url = "jdbc:singlestore://master:3306,child1:3307,child2:3308?autoReconnect=truee";
    Properties prop = new Properties();
    prop.setProperty("user", "greg");
    prop.setProperty("password", "pass");
    try {
      Configuration.parse(url, prop);
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage()
              .contains(
                  "Optional parameter autoReconnect must be boolean (true/false or 0/1) was \"truee\""));
    }
  }

  @Test
  public void testJdbcParserSimpleIpv4() throws SQLException {
    String url =
        "jdbc:singlestore://master:3306,child1:3307,child2:3308/database?user=greg&password=pass";
    Configuration conf = com.singlestore.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("child1", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child2", 3308), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserSimpleIpv6() throws SQLException {
    String url =
        "jdbc:singlestore://[2001:0660:7401:0200:0000:0000:0edf:bdd7],[2001:660:7401:200::edf:bdd7]:3307,[2001:660:7401:200::edf:bdd7]-test"
            + "/database?user=greg&password=pass";
    Configuration conf = com.singlestore.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(
        HostAddress.from("2001:0660:7401:0200:0000:0000:0edf:bdd7", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("2001:660:7401:200::edf:bdd7", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("2001:660:7401:200::edf:bdd7", 3306), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserParameter() throws SQLException {
    String url =
        "jdbc:singlestore://address=(port=3306)(host=master1),address=(port=3307)"
            + "(host=master2),address=(host=child1)(port=3308)/database?user=greg&password=pass";
    Configuration conf = com.singlestore.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("master2", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child1", 3308), conf.addresses().get(2));

    url =
        "jdbc:singlestore://address=(port=3306)(host=master1),address=(port=3307)"
            + "(host=master2),address=(host=master3)(port=3308)/database?user=greg&password=pass";
    conf = com.singlestore.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("master2", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("master3", 3308), conf.addresses().get(2));

    url =
        "jdbc:singlestore:loadbalance://address=(port=3306)(host=master1),address=(port=3307)"
            + "(host=child1) ,address=(host=child2)(port=3308)(other=5/database?user=greg&password=pass";
    conf = com.singlestore.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("child1", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child2", 3308), conf.addresses().get(2));
  }

  @Test
  public void address() {
    assertEquals("address=(host=test)(port=3306)", HostAddress.from("test", 3306).toString());
    assertEquals("address=(host=test)(port=3306)", HostAddress.from("test", 3306).toString());
    assertEquals("address=(host=test)(port=3306)", HostAddress.from("test", 3306).toString());
  }

  @Test
  public void hostAddressEqual() {
    HostAddress host = HostAddress.from("test", 3306);
    assertEquals(host, host);
    assertNotEquals(null, host);
    assertEquals(HostAddress.from("test", 3306), host);
    assertNotEquals("", host);
    assertNotEquals(HostAddress.from("test2", 3306), host);
    assertNotEquals(HostAddress.from("test", 3307), host);
  }

  @Test
  public void testJdbcParserParameterErrorEqual() {
    String wrongIntVal = "jdbc:singlestore://localhost?socketTimeout=blabla";
    assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(wrongIntVal),
        "Optional parameter socketTimeout must be Integer, was 'blabla'");
    String wrongBoolVal = "jdbc:singlestore://localhost?autocommit=blabla";
    assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(wrongBoolVal),
        "Optional parameter autocommit must be boolean (true/false or 0/1)");
  }

  @Test
  public void testJdbcParserHaModeNone() throws SQLException {
    String url = "jdbc:singlestore://localhost/database";
    Configuration jdbc = Configuration.parse(url);
    assertTrue(jdbc.haMode().equals(HaMode.NONE));
  }

  @Test
  public void testJdbcParserHaModeLoad() throws SQLException {
    String url = "jdbc:singlestore:sequential://localhost/database";
    Configuration jdbc = Configuration.parse(url);
    assertTrue(jdbc.haMode().equals(HaMode.SEQUENTIAL));
  }

  @Test
  public void testJdbcParserLoadBalanceParameter() throws SQLException {
    String url =
        "jdbc:singlestore:loadbalance://address=(port=3306)(host=master1),address=(port=3307)"
            + "(host=master2),address=(host=child1)(port=3308)/database"
            + "?user=greg&password=pass&pinGlobalTxToPhysicalConnection&servicePrincipalName=BLA";
    Configuration conf = com.singlestore.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals("BLA", conf.servicePrincipalName());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("master2", 3307), conf.addresses().get(1));
    assertEquals(HostAddress.from("child1", 3308), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserLoadBalanceParameterWithoutType() throws SQLException {
    String url = "jdbc:singlestore:loadbalance://master1,child1,child2/database";
    Configuration conf = com.singlestore.jdbc.Configuration.parse(url);
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306), conf.addresses().get(0));
    assertEquals(HostAddress.from("child1", 3306), conf.addresses().get(1));
    assertEquals(HostAddress.from("child2", 3306), conf.addresses().get(2));
  }

  /**
   * Conj-167 : Driver is throwing IllegalArgumentException instead of returning null.
   *
   * @throws SQLException if any exception occur
   */
  @Test
  public void checkOtherDriverCompatibility() throws SQLException {
    Configuration jdbc = Configuration.parse("jdbc:h2:mem:RZM;DB_CLOSE_DELAY=-1");
    assertTrue(jdbc == null);
  }

  @Test
  public void checkDisable() throws SQLException {
    Configuration jdbc = Configuration.parse("jdbc:mysql://localhost/test");
    assertTrue(jdbc == null);
  }

  @Test
  public void loginTimeout() throws SQLException {
    Configuration jdbc = Configuration.parse("jdbc:singlestore://localhost/test");
    assertEquals(30000, jdbc.connectTimeout());

    DriverManager.setLoginTimeout(10);
    jdbc = Configuration.parse("jdbc:singlestore://localhost/test");
    assertEquals(10000, jdbc.connectTimeout());

    jdbc = Configuration.parse("jdbc:singlestore://localhost/test?connectTimeout=5000");
    assertEquals(5000, jdbc.connectTimeout());
    DriverManager.setLoginTimeout(0);

    jdbc = Configuration.parse("jdbc:singlestore://localhost/test?connectTimeout=5000");
    assertEquals(5000, jdbc.connectTimeout());
  }

  @Test
  public void checkHaMode() throws SQLException {
    checkHaMode("jdbc:singlestore://localhost/test", HaMode.NONE);
    checkHaMode("jdbc:singlestore:sequential://localhost/test", HaMode.SEQUENTIAL);
    checkHaMode("jdbc:singlestore:sequential//localhost/test", HaMode.SEQUENTIAL);
    checkHaMode("jdbc:singlestore:failover://localhost:3306/test", HaMode.LOADBALANCE);
    checkHaMode("jdbc:singlestore:loadbalance://localhost:3306/test", HaMode.LOADBALANCE);

    try {
      checkHaMode("jdbc:singlestore:sequent//localhost/test", HaMode.SEQUENTIAL);
      fail();
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("wrong failover parameter format in connection String"));
    }
  }

  private void checkHaMode(String url, HaMode expectedHaMode) throws SQLException {
    Configuration jdbc = Configuration.parse(url);
    assertEquals(expectedHaMode, jdbc.haMode());
  }

  /**
   * CONJ-452 : correcting line break in connection url.
   *
   * @throws SQLException if any exception occur
   */
  @Test
  public void checkInfileCertificate() throws SQLException {
    String url =
        "jdbc:singlestore://1.2.3.4/testj?user=diego"
            + "&autocommit=true&serverSslCert="
            + "-----BEGIN CERTIFICATE-----\n"
            + "MIIDITCCAgmgAwIBAgIBADANBgkqhkiG9w0BAQUFADBIMSMwIQYDVQQDExpHb29n\n"
            + "bGUgQ2xvdWQgU1FMIFNlcnZlciBDQTEUMBIGA1UEChMLR29vZ2xlLCBJbmMxCzAJ\n"
            + "BgNVBAYTAlVTMB4XDTE3MDQyNzEyMjcyNFoXDTE5MDQyNzEyMjgyNFowSDEjMCEG\n"
            + "A1UEAxMaR29vZ2xlIENsb3VkIFNRTCBTZXJ2ZXIgQ0ExFDASBgNVBAoTC0dvb2ds\n"
            + "ZSwgSW5jMQswCQYDVQQGEwJVUzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
            + "ggEBANA6vS37//gvYOsEXKA9Cnlt/C1Ef/a5zJNahpVxx8HbJn6DF+pQbhHm3o4P\n"
            + "TeZp1HoRg5TRiXOEkNTBmgSQbR2+otM2q2gmkn4XAh0+yXkNW3hr2IydJyg9C26v\n"
            + "/OzFvuLcw9iDBvrn433pDa6vjYDU+wiQaVtr1ItzsoE/kgW2IkgFVQB+CrkpAmwm\n"
            + "omwEze3QFUUznP0PHy3P7g7UVD9u5x3APY6kVt2dq8mnOiLZkyfHHR2j6+j0E73I\n"
            + "k3HQv7D0yRIv3kuNpFgJbITVgDIq9ukWU2XinDHUjguCDH+yQAoQH7hOQlWUHIz8\n"
            + "/TtfZjrlUQf2uLzOWCn5KxfEqTkCAwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIB\n"
            + "ADANBgkqhkiG9w0BAQUFAAOCAQEArYkBkUvMvteT4fN6lxUkzmp8R7clLPkA2HnJ\n"
            + "7IUK3c5GJ0/blffxj/9Oe2g+etga15SIO73GNAnTxhxIJllKRmmh6SR+dwNMkAvE\n"
            + "xp87/Y6cSeJp5d4HhZUvxpFjaUDsWIC8tpbriUJoqGIirprLVcsPgDjKyuvVOlbK\n"
            + "aQf3fOoBPLspGWHgic8Iw1O4kRcStUGCSCwOtYcgMJEhVqTgX0sTX5BgatZhr8FY\n"
            + "Mnoceo2vzzxgHJU9qZuPkpYDs+ipQjzhoIJaY4HU2Uz4jMptqxSdzsPpC6PAKwuN\n"
            + "+LBCR0B194YbRn6726vWwUUE05yskVN6gllGSCgZ/G8y98DhjQ==\n"
            + "-----END CERTIFICATE-----&sslMode&password=testj&password=pwd2";
    Configuration jdbc = Configuration.parse(url);
    assertEquals("diego", jdbc.user());
    assertEquals(true, jdbc.autocommit());
    assertEquals(
        "-----BEGIN CERTIFICATE-----\n"
            + "MIIDITCCAgmgAwIBAgIBADANBgkqhkiG9w0BAQUFADBIMSMwIQYDVQQDExpHb29n\n"
            + "bGUgQ2xvdWQgU1FMIFNlcnZlciBDQTEUMBIGA1UEChMLR29vZ2xlLCBJbmMxCzAJ\n"
            + "BgNVBAYTAlVTMB4XDTE3MDQyNzEyMjcyNFoXDTE5MDQyNzEyMjgyNFowSDEjMCEG\n"
            + "A1UEAxMaR29vZ2xlIENsb3VkIFNRTCBTZXJ2ZXIgQ0ExFDASBgNVBAoTC0dvb2ds\n"
            + "ZSwgSW5jMQswCQYDVQQGEwJVUzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
            + "ggEBANA6vS37//gvYOsEXKA9Cnlt/C1Ef/a5zJNahpVxx8HbJn6DF+pQbhHm3o4P\n"
            + "TeZp1HoRg5TRiXOEkNTBmgSQbR2+otM2q2gmkn4XAh0+yXkNW3hr2IydJyg9C26v\n"
            + "/OzFvuLcw9iDBvrn433pDa6vjYDU+wiQaVtr1ItzsoE/kgW2IkgFVQB+CrkpAmwm\n"
            + "omwEze3QFUUznP0PHy3P7g7UVD9u5x3APY6kVt2dq8mnOiLZkyfHHR2j6+j0E73I\n"
            + "k3HQv7D0yRIv3kuNpFgJbITVgDIq9ukWU2XinDHUjguCDH+yQAoQH7hOQlWUHIz8\n"
            + "/TtfZjrlUQf2uLzOWCn5KxfEqTkCAwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIB\n"
            + "ADANBgkqhkiG9w0BAQUFAAOCAQEArYkBkUvMvteT4fN6lxUkzmp8R7clLPkA2HnJ\n"
            + "7IUK3c5GJ0/blffxj/9Oe2g+etga15SIO73GNAnTxhxIJllKRmmh6SR+dwNMkAvE\n"
            + "xp87/Y6cSeJp5d4HhZUvxpFjaUDsWIC8tpbriUJoqGIirprLVcsPgDjKyuvVOlbK\n"
            + "aQf3fOoBPLspGWHgic8Iw1O4kRcStUGCSCwOtYcgMJEhVqTgX0sTX5BgatZhr8FY\n"
            + "Mnoceo2vzzxgHJU9qZuPkpYDs+ipQjzhoIJaY4HU2Uz4jMptqxSdzsPpC6PAKwuN\n"
            + "+LBCR0B194YbRn6726vWwUUE05yskVN6gllGSCgZ/G8y98DhjQ==\n"
            + "-----END CERTIFICATE-----",
        jdbc.serverSslCert());
    assertEquals(SslMode.DISABLE, jdbc.sslMode());
    assertEquals("pwd2", jdbc.password());
  }

  @Test
  public void builder() {
    Configuration conf =
        new Configuration.Builder()
            .addresses(HostAddress.from("host1", 3305), HostAddress.from("host2", 3307))
            .user("me")
            .password("pwd")
            .database("db")
            .socketFactory("someSocketFactory")
            .connectTimeout(22)
            .restrictedAuth("mysql_native_password")
            .pipe("pipeName")
            .localSocket("localSocket")
            .tcpKeepAlive(false)
            .tcpAbortiveClose(true)
            .localSocketAddress("localSocketAddress")
            .socketTimeout(1000)
            .allowMultiQueries(true)
            .allowLocalInfile(true)
            .useCompression(true)
            .blankTableNameMeta(true)
            .credentialType("ENV")
            .sslMode("REQUIRED")
            .enabledSslCipherSuites("myCipher,cipher2")
            .sessionVariables("blabla")
            .tinyInt1isBit(false)
            .yearIsDateType(false)
            .dumpQueriesOnException(true)
            .prepStmtCacheSize(2)
            .useAffectedRows(true)
            .useServerPrepStmts(true)
            .connectionAttributes("bla=bla")
            .includeThreadDumpInDeadlockExceptions(true)
            .servicePrincipalName("SPN")
            .defaultFetchSize(10)
            .tlsSocketType("TLStype")
            .maxQuerySizeToLog(100)
            .retriesAllDown(10)
            .enabledSslProtocolSuites("TLSv1.2")
            .transactionReplay(true)
            .pool(true)
            .autocommit(false)
            .poolName("myPool")
            .maxPoolSize(16)
            .minPoolSize(12)
            .maxIdleTime(25000)
            .transactionIsolation("READ-COMMITTED")
            .keyStore("/tmp")
            .keyStorePassword("MyPWD")
            .keyStoreType("JKS")
            .geometryDefaultType("default")
            .registerJmxPool(false)
            .tcpKeepCount(50)
            .tcpKeepIdle(10)
            .tcpKeepInterval(50)
            .poolValidMinDelay(260)
            .useResetConnection(true)
            .nullDatabaseMeansCurrent(true)
            .cachePrepStmts(false)
            .serverSslCert("mycertPath")
            .build();
    assertEquals(
        "jdbc:singlestore://host1:3305,address=(host=host2)(port=3307)/db?user=me&password=***&autocommit=false&nullDatabaseMeansCurrent=true&defaultFetchSize=10&geometryDefaultType=default&restrictedAuth=mysql_native_password&socketFactory=someSocketFactory&connectTimeout=22&pipe=pipeName&localSocket=localSocket&tcpKeepAlive=false&tcpKeepIdle=10&tcpKeepCount=50&tcpKeepInterval=50&tcpAbortiveClose=true&localSocketAddress=localSocketAddress&socketTimeout=1000&tlsSocketType=TLStype&sslMode=TRUST&serverSslCert=mycertPath&keyStore=/tmp&keyStorePassword=***&keyStoreType=JKS&enabledSslCipherSuites=myCipher,cipher2&enabledSslProtocolSuites=TLSv1.2&allowMultiQueries=true&useCompression=true&useAffectedRows=true&cachePrepStmts=false&prepStmtCacheSize=2&useServerPrepStmts=true&credentialType=ENV&sessionVariables=blabla&connectionAttributes=bla=bla&servicePrincipalName=SPN&blankTableNameMeta=true&tinyInt1isBit=false&yearIsDateType=false&dumpQueriesOnException=true&includeThreadDumpInDeadlockExceptions=true&retriesAllDown=10&transactionReplay=true&pool=true&poolName=myPool&maxPoolSize=16&minPoolSize=12&maxIdleTime=25000&registerJmxPool=false&poolValidMinDelay=260&useResetConnection=true&maxQuerySizeToLog=100",
        conf.toString());
  }

  @Test
  public void equal() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test");
    assertEquals(conf, conf);
    assertEquals(Configuration.parse("jdbc:singlestore://localhost/test"), conf);
    assertNotEquals(null, conf);
    assertNotEquals("", conf);
    assertNotEquals(Configuration.parse("jdbc:singlestore://localhost/test2"), conf);
  }

  @Test
  public void toConf() throws SQLException {
    assertTrue(
        Configuration.toConf("jdbc:singlestore://localhost/test")
            .startsWith(
                "Configuration:\n"
                    + " * resulting Url : jdbc:singlestore://localhost/test\n"
                    + "Unknown options : None\n"
                    + "\n"
                    + "Non default options : \n"
                    + " * database : test\n"
                    + "\n"
                    + "default options :"));
    assertTrue(
        normalizeConfigurationString(
                Configuration.toConf(
                    "jdbc:singlestore:loadbalance://host1:3305,address=(host=host2)(port=3307)(type=replica)/db?nonExisting&nonExistingWithValue=tt&user=me&password=***&autocommit=false&createDatabaseIfNotExist=true&"))
            .startsWith(
                "Configuration:\n"
                    + " * resulting Url :"
                    + " "
                    + normalizeConnectionString(
                        "jdbc:singlestore:loadbalance://host1:3305,address=(host=host2)(port=3307)/db?user=me&password=***&nonExisting=&nonExistingWithValue=tt&autocommit=false&createDatabaseIfNotExist=true")
                    + "\n"
                    + "Unknown options : \n"
                    + " * nonExisting : \n"
                    + " * nonExistingWithValue : tt\n"
                    + "\n"
                    + "Non default options : \n"
                    + " * addresses : [address=(host=host1)(port=3305), address=(host=host2)(port=3307)]\n"
                    + " * autocommit : false\n"
                    + " * createDatabaseIfNotExist : true\n"
                    + " * database : db\n"
                    + " * haMode : LOADBALANCE\n"
                    + " * password : ***\n"
                    + " * user : me\n"
                    + "\n"
                    + "default options :\n"
                    + " * allowLocalInfile : true\n"
                    + " * allowMultiQueries : false\n"
                    + " * blankTableNameMeta : false"));

    assertTrue(
        normalizeConfigurationString(
                Configuration.toConf(
                    "jdbc:singlestore://localhost/test?user=root&sslMode=verify-ca&serverSslCert=/tmp/t.pem&trustStoreType=JKS&keyStore=/tmp/keystore&keyStorePassword=kspass"))
            .startsWith(
                "Configuration:\n"
                    + " * resulting Url :"
                    + " "
                    + normalizeConnectionString(
                        "jdbc:singlestore://localhost/test?user=root&sslMode=VERIFY_CA&serverSslCert=/tmp/t.pem&keyStore=/tmp/keystore&keyStorePassword=***&trustStoreType=JKS")
                    + "\n"
                    + "Unknown options : None\n"
                    + "\n"
                    + "Non default options : \n"
                    + " * database : test\n"
                    + " * keyStore : /tmp/keystore\n"
                    + " * keyStorePassword : ***\n"
                    + " * serverSslCert : /tmp/t.pem\n"
                    + " * sslMode : VERIFY_CA\n"
                    + " * trustStoreType : JKS\n"
                    + " * user : root\n"
                    + "\n"
                    + "default options :\n"
                    + " * addresses : [address=(host=localhost)(port=3306)]\n"
                    + " * allowLocalInfile : true"));
  }

  private String normalizeConfigurationString(String configString) {
    String[] lines = configString.split("\n");
    StringBuilder newConfig = new StringBuilder();
    for (String line : lines) {
      if (line.startsWith(" * resulting Url :")) {
        String url = line.substring(" * resulting Url :".length()).trim();
        newConfig.append(" * resulting Url : ").append(normalizeConnectionString(url)).append("\n");
      } else {
        newConfig.append(line).append("\n");
      }
    }
    return newConfig.toString();
  }

  private String normalizeConnectionString(String connectionString) {
    String[] parts = connectionString.split("\\?", 2);
    if (parts.length < 2) {
      return connectionString;
    }
    List<String> paramList = Arrays.asList(parts[1].split("&"));
    String sortedParams =
        paramList.stream()
            .map(param -> param.contains("=") ? param : param + "=")
            .sorted()
            .collect(Collectors.joining("&"));
    return parts[0] + "?" + sortedParams;
  }
}
