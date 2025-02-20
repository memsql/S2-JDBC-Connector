// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.*;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.integration.tools.TcpProxy;
import java.io.*;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.sql.*;
import java.util.*;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.*;

@DisplayName("SSL tests")
public class SslTest extends Common {
  private static Integer sslPort = port;
  private static String baseOptions = "&user=serverAuthUser&password=!Passw0rd3Works";
  private static final String baseMutualOptions = "&user=mutualAuthUser&password=!Passw0rd3Works";

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
  }

  public static String retrieveCertificatePath() throws Exception {
    String serverCertificatePath = checkFileExists(System.getProperty("serverCertificatePath"));
    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists(System.getenv("TEST_DB_SERVER_CERT"));
    }

    // try local server
    if (serverCertificatePath == null) {

      try (ResultSet rs = sharedConn.createStatement().executeQuery("select @@ssl_cert")) {
        assertTrue(rs.next());
        serverCertificatePath = checkFileExists(rs.getString(1));
      }
    }
    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists("src/test/resources/ssl/server.crt");
    }
    return serverCertificatePath;
  }

  private static String checkFileExists(String path) throws IOException {
    if (path == null) return null;
    File f = new File(path);
    if (f.exists()) {
      return f.getCanonicalPath().replace("\\", "/");
    }
    return null;
  }

  private static void createSslUser(String user, String requirement) throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE USER '" + user + "'@'%' IDENTIFIED BY '!Passw0rd3Works' " + requirement);
    stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");
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
    try {
      List<String> protocols =
          Arrays.asList(SSLContext.getDefault().getSupportedSSLParameters().getProtocols());
      Assumptions.assumeTrue(protocols.contains("TLSv1.3") && protocols.contains("TLSv1.2"));
    } catch (NoSuchAlgorithmException e) {
      // eat
    }
    try (Connection con =
        createCon(
            baseOptions + "&sslMode=trust&enabledSslProtocolSuites=TLSv1.2,TLSv1.3", sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    Common.assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () ->
            createCon(baseMutualOptions + "&sslMode=trust&enabledSslProtocolSuites=SSLv3", sslPort),
        "No appropriate protocol");
    Common.assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseMutualOptions + "&sslMode=trust&enabledSslProtocolSuites=unknown", sslPort),
        "Unsupported SSL protocol 'unknown'");
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
    Common.assertThrowsContains(
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
      Common.assertThrowsContains(
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

    Common.assertThrowsContains(
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

  @Test
  public void trustStoreMandatorySsl() throws Throwable {
    String trustStorePath = checkAndCanonizePath("scripts/ssl/test-TS.jks");
    Assumptions.assumeTrue(trustStorePath != null, "Canceled, server certificate not provided");

    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=VERIFY_CA&trustStore="
                + trustStorePath
                + "&trustStorePassword=trustPass",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
  }

  public static String retrieveCaCertificatePath() throws Exception {
    String serverCertificatePath = checkFileExists(System.getProperty("serverCertificatePath"));
    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists(System.getenv("TEST_DB_SERVER_CA_CERT"));
    }

    if (serverCertificatePath == null) {
      serverCertificatePath = checkFileExists("src/test/resources/ssl/ca.crt");
    }
    return serverCertificatePath;
  }

  @Test
  public void trustStoreParameter() throws Throwable {
    String serverCertPath = retrieveCertificatePath();
    String caCertPath = retrieveCaCertificatePath();
    Assumptions.assumeTrue(serverCertPath != null, "Canceled, server certificate not provided");

    KeyStore ks = KeyStore.getInstance("jks");
    char[] pwdArray = "myPwd0".toCharArray();
    ks.load(null, pwdArray);

    File temptrustStoreFile = File.createTempFile("newKeyStoreFileName", ".jks");

    KeyStore ks2 = KeyStore.getInstance("pkcs12");
    ks2.load(null, pwdArray);
    File temptrustStoreFile2 = File.createTempFile("newKeyStoreFileName", ".pkcs12");

    try (InputStream inStream = new File(serverCertPath).toURI().toURL().openStream()) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Collection<? extends Certificate> serverCertList = cf.generateCertificates(inStream);
      List<Certificate> certs = new ArrayList<>();
      for (Iterator<? extends Certificate> iter = serverCertList.iterator(); iter.hasNext(); ) {
        certs.add(iter.next());
      }
      if (caCertPath != null) {
        try (InputStream inStream2 = new File(caCertPath).toURI().toURL().openStream()) {
          CertificateFactory cf2 = CertificateFactory.getInstance("X.509");
          Collection<? extends Certificate> caCertList = cf2.generateCertificates(inStream2);
          for (Iterator<? extends Certificate> iter = caCertList.iterator(); iter.hasNext(); ) {
            certs.add(iter.next());
          }
        }
      }
      for (Certificate cert : certs) {
        ks.setCertificateEntry(hostname, cert);
        ks2.setCertificateEntry(hostname, cert);
      }

      try (FileOutputStream fos = new FileOutputStream(temptrustStoreFile.getPath())) {
        ks.store(fos, pwdArray);
      }
      try (FileOutputStream fos = new FileOutputStream(temptrustStoreFile2.getPath())) {
        ks2.store(fos, pwdArray);
      }
    }

    // certificate path, like  /path/certificate.crt
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=VERIFY_CA&trustStore="
                + temptrustStoreFile
                + "&trustStoreType=jks&trustStorePassword=myPwd0",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }

    // with alias
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=VERIFY_CA&trustCertificateKeystoreUrl="
                + temptrustStoreFile
                + "&trustCertificateKeyStoretype=jks&trustCertificateKeystorePassword=myPwd0",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseOptions
                    + "&sslMode=VERIFY_CA&trustStore="
                    + temptrustStoreFile
                    + "&trustStoreType=jks&trustStorePassword=wrongPwd",
                sslPort),
        "Failed load keyStore");
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseOptions
                    + "&sslMode=VERIFY_CA&trustCertificateKeystoreUrl="
                    + temptrustStoreFile
                    + "&trustCertificateKeyStoretype=jks&trustCertificateKeystorePassword=wrongPwd",
                sslPort),
        "Failed load keyStore");
    try (Connection con =
        createCon(
            baseOptions
                + "&sslMode=VERIFY_CA&trustStore="
                + temptrustStoreFile2
                + "&trustStoreType=pkcs12&trustStorePassword=myPwd0",
            sslPort)) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                baseOptions
                    + "&sslMode=VERIFY_CA&trustStore="
                    + temptrustStoreFile2
                    + "&trustStoreType=pkcs12&trustStorePassword=wrongPwd",
                sslPort),
        "Failed load keyStore");
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
