// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.*;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.integration.tools.TcpProxy;
import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import org.junit.jupiter.api.*;

@DisplayName("SSL tests")
public class SslTest extends Common {
  private static Integer sslPort;
  private static String baseOptions = "&user=serverAuthUser&password=!Passw0rd3Works";

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS serverAuthUser");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Assumptions.assumeTrue(haveSsl());
    createSslUser("serverAuthUser", "REQUIRE SSL");

    Statement stmt = sharedConn.createStatement();
    stmt.execute("FLUSH PRIVILEGES");
    sslPort =
        System.getenv("TEST_MAXSCALE_TLS_PORT") == null
                || System.getenv("TEST_MAXSCALE_TLS_PORT").isEmpty()
            ? null
            : Integer.valueOf(System.getenv("TEST_MAXSCALE_TLS_PORT"));
  }

  private static void createSslUser(String user, String requirement) throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "GRANT SELECT ON *.* TO '" + user + "'@'%' IDENTIFIED BY '!Passw0rd3Works' " + requirement);
  }

  private String getSslVersion(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("show STATUS  LIKE 'Ssl_version'");
    if (rs.next()) {
      return rs.getString(2);
    }
    return null;
  }

  @Test
  public void simpleSsl() throws SQLException {
    try (Connection con = createCon("sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con = createCon("sslMode=trust&useReadAheadInput=false", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
  }

  @Test
  public void mandatorySsl() throws SQLException {
    try (Connection con = createCon(baseOptions + "&sslMode=trust", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrows(SQLException.class, () -> createCon(baseOptions + "&sslMode=disable"));
  }

  @Test
  public void enabledSslProtocolSuites() throws SQLException {
    try (Connection con =
        createCon(
            baseOptions + "&sslMode=trust&enabledSslProtocolSuites=TLSv1.2,TLSv1.3", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
  }

  @Test
  public void enabledSslCipherSuites() throws SQLException {
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=trust&enabledSslCipherSuites=TLS_RSA_WITH_AES_256_GCM_SHA384,TLS_RSA_WITH_AES_128_GCM_SHA256",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseOptions + "&sslMode=trust&enabledSslCipherSuites=UNKNOWN_CIPHER", sslPort),
        "Unsupported SSL cipher");
  }

  @Test
  public void certificateMandatorySsl() throws Throwable {

    String serverCertPath = retrieveCertificatePath();
    Assumptions.assumeTrue(serverCertPath != null, "Canceled, server certificate not provided");

    // certificate path, like  /path/certificate.crt
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=" + serverCertPath, sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    if (!"localhost".equals(hostname)) {
      try (Connection con =
          createCon(
              baseOptions + "&sslMode=VERIFY_FULL&serverSslCert=" + serverCertPath, sslPort)) {
        assertNotNull(getSslVersion(con));
      }

      Configuration conf = Configuration.parse(mDefUrl);
      HostAddress hostAddress = conf.addresses().get(0);
      try {
        proxy = new TcpProxy(hostAddress.host, sslPort == null ? hostAddress.port : sslPort);
      } catch (IOException i) {
        throw new SQLException("proxy error", i);
      }

      String url = mDefUrl.replaceAll("//([^/]*)/", "//localhost:" + proxy.getLocalPort() + "/");
      assertThrowsContains(
          SQLException.class,
          () ->
              DriverManager.getConnection(
                  url + "&sslMode=VERIFY_FULL&serverSslCert=" + serverCertPath),
          "DNS host \"localhost\" doesn't correspond to certificate");
    }

    String urlPath = Paths.get(serverCertPath).toUri().toURL().toString();
    // file certificate path, like  file:/path/certificate.crt
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=" + urlPath, sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    assertThrowsContains(
        SQLException.class,
        () -> createCon(baseOptions + "&sslMode=VERIFY_FULL&serverSslCert=" + urlPath, sslPort),
        "DNS host \"localhost\" doesn't correspond to certificate CN \"test-memsql-server\"");

    String certificateString = getServerCertificate(serverCertPath);
    // file certificate, like  -----BEGIN CERTIFICATE-----...
    try (Connection con =
        createCon(baseOptions + "&sslMode=VERIFY_CA&serverSslCert=" + certificateString, sslPort)) {
      assertNotNull(getSslVersion(con));
    }
  }

  private String getServerCertificate(String serverCertPath) throws SQLException {
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(serverCertPath)))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
      return sb.toString();
    } catch (Exception e) {
      throw new SQLException("abnormal exception", e);
    }
  }
}
