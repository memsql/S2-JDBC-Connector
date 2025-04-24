// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import java.io.*;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.*;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.*;

@DisplayName("SSL tests")
public class SslTest extends Common {
  private static final String BASE_OPTIONS = "&user=serverAuthUser&password=!Passw0rd3Works";
  private static final String BASE_MUTUAL_OPTIONS = "&user=mutualAuthUser&password=!Passw0rd3Works";

  public static final String CA_CERT_PATH = "scripts/ssl/ca-cert.pem";
  public static final String SERVER_CERT_PATH = "scripts/ssl/server-cert.pem";
  public static final String TRUST_STORE_PATH = "scripts/ssl/truststore.jks";
  public static final String CLIENT_KEY_STORE_PATH = "scripts/ssl/client-keystore.p12";
  public static final String KEY_STORE_PASSWORD = "password";
  public static final String TRUST_STORE_PASSWORD = "password";

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS serverAuthUser");
    if (haveMutualSsl()) {
      stmt.execute("DROP USER IF EXISTS mutualAuthUser");
    }
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Assumptions.assumeTrue(haveSsl());
    createSslUser("serverAuthUser", "REQUIRE SSL");
    if (haveMutualSsl()) {
      createSslUser("mutualAuthUser", "REQUIRE X509");
    }
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

  private static boolean haveMutualSsl() {
    return minVersion(9, 0, 1);
  }

  private static String checkFileExists(String path) throws IOException {
    if (path == null) return null;
    File f = new File(path);
    if (f.exists()) {
      return f.getCanonicalPath().replace("\\", "/");
    }
    return null;
  }

  @Test
  public void simpleSsl() throws SQLException {
    try (Connection con = createCon("sslMode=trust")) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con = createCon("sslMode=trust&useReadAheadInput=false")) {
      assertNotNull(getSslVersion(con));
    }
  }

  @Test
  public void errorUsingWrongTypeOfKeystore() throws Exception {
    Assumptions.assumeTrue(haveMutualSsl());
    // wrong keystore type
    assertThrows(
        SQLNonTransientConnectionException.class,
        () ->
            createCon(
                BASE_MUTUAL_OPTIONS
                    + "&sslMode=verify-ca&serverSslCert="
                    + SERVER_CERT_PATH
                    + "&keyStoreType=JCEKS&keyStore="
                    + CLIENT_KEY_STORE_PATH
                    + "&keyStorePassword="
                    + KEY_STORE_PASSWORD));
  }

  @Test
  public void mutualAuthSsl() throws Exception {
    Assumptions.assumeTrue(haveMutualSsl());
    // without password
    try {
      assertThrows(
          java.sql.SQLException.class, // expected SQLInvalidAuthorizationSpecException
          () ->
              createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust&keyStore=" + CLIENT_KEY_STORE_PATH));
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    // with password
    try (Connection con =
        createCon(
            BASE_MUTUAL_OPTIONS
                + "&sslMode=trust&keyStore="
                + CLIENT_KEY_STORE_PATH
                + "&keyStorePassword="
                + KEY_STORE_PASSWORD)) {
      assertNotNull(getSslVersion(con));
    }

    String clientKeyStoreFullPath = checkFileExists(CLIENT_KEY_STORE_PATH);
    // with URL
    boolean isWin = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
    try (Connection con =
        createCon(
            BASE_MUTUAL_OPTIONS
                + "&sslMode=trust&keyStore="
                + "file://"
                + (isWin ? "/" : "")
                + clientKeyStoreFullPath
                + "&keyStorePassword="
                + KEY_STORE_PASSWORD)) {
      assertNotNull(getSslVersion(con));
    }

    String prevValue = System.getProperty("javax.net.ssl.keyStore");
    String prevPwdValue = System.getProperty("javax.net.ssl.keyStorePassword");
    System.setProperty(
        "javax.net.ssl.keyStore", "file://" + (isWin ? "/" : "") + clientKeyStoreFullPath);
    System.setProperty("javax.net.ssl.keyStorePassword", KEY_STORE_PASSWORD);
    try {
      try (Connection con = createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust")) {
        assertNotNull(getSslVersion(con));
      }
    } finally {
      if (prevValue == null) {
        System.clearProperty("javax.net.ssl.keyStore");
      } else {
        System.setProperty("javax.net.ssl.keyStore", prevValue);
      }
      if (prevPwdValue == null) {
        System.clearProperty("javax.net.ssl.keyStorePassword");
      } else {
        System.setProperty("javax.net.ssl.keyStorePassword", prevPwdValue);
      }
    }

    // wrong keystore type
    assertThrows(
        SQLException.class, // expected SQLInvalidAuthorizationSpecException
        () ->
            createCon(
                BASE_MUTUAL_OPTIONS
                    + "&sslMode=trust&keyStoreType=JKS&keyStore="
                    + CLIENT_KEY_STORE_PATH));
    // good keystore type
    try (Connection con =
        createCon(
            BASE_MUTUAL_OPTIONS
                + "&sslMode=trust&keyStoreType=pkcs12&keyStore="
                + CLIENT_KEY_STORE_PATH
                + "&keyStorePassword="
                + KEY_STORE_PASSWORD)) {
      assertNotNull(getSslVersion(con));
    }

    // with system properties
    System.setProperty("javax.net.ssl.keyStore", clientKeyStoreFullPath);
    System.setProperty("javax.net.ssl.keyStorePassword", KEY_STORE_PASSWORD);
    try (Connection con = createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust")) {
      assertNotNull(getSslVersion(con));
    }

    System.setProperty("javax.net.ssl.keyStoreType", "JKS");
    try (Connection con = createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust")) {
      assertNotNull(getSslVersion(con));
    }
    try (Connection con = createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust")) {
      assertNotNull(getSslVersion(con));
    }

    System.clearProperty("javax.net.ssl.keyStoreType");
    try (Connection con = createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust&keyStoreType=JKS")) {
      assertNotNull(getSslVersion(con));
    }

    // without password
    System.clearProperty("javax.net.ssl.keyStorePassword");
    assertThrows(
        SQLException.class, // expected SQLInvalidAuthorizationSpecException
        () -> createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust"));
  }

  @Test
  public void mandatorySsl() throws SQLException {
    try (Connection con = createCon(BASE_OPTIONS + "&sslMode=trust")) {
      assertNotNull(getSslVersion(con));
    }
    assertThrows(SQLException.class, () -> createCon(BASE_OPTIONS + "&sslMode=disable"));
    if (haveMutualSsl()) {
      assertThrows(
          SQLException.class, // expected SQLInvalidAuthorizationSpecException
          () -> createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust"));
    }
  }

  @Test
  public void enabledSslProtocolSuites() throws SQLException {
    try {
      List<String> protocols =
          Arrays.asList(SSLContext.getDefault().getSupportedSSLParameters().getProtocols());
      Assumptions.assumeTrue(protocols.contains("TLSv1.2")); // SingleStore doesn't support TLSv1.3
    } catch (NoSuchAlgorithmException e) {
      // eat
    }
    try (Connection con =
        createCon(BASE_OPTIONS + "&sslMode=trust&enabledSslProtocolSuites=TLSv1.2")) {
      assertNotNull(getSslVersion(con));
    }
    if (haveMutualSsl()) {
      Common.assertThrowsContains(
          SQLNonTransientConnectionException.class,
          () -> createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust&enabledSslProtocolSuites=SSLv3"),
          "No appropriate protocol");
      Common.assertThrowsContains(
          SQLException.class,
          () -> createCon(BASE_MUTUAL_OPTIONS + "&sslMode=trust&enabledSslProtocolSuites=unknown"),
          "Unsupported SSL protocol 'unknown'");
    }
  }

  @Test
  public void enabledSslCipherSuites() throws SQLException {
    try (Connection con =
        createCon(
            BASE_OPTIONS
                + "&sslMode=trust&enabledSslCipherSuites=TLS_RSA_WITH_AES_256_GCM_SHA384,TLS_RSA_WITH_AES_128_GCM_SHA256")) {
      assertNotNull(getSslVersion(con));
    }
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon(BASE_OPTIONS + "&sslMode=trust&enabledSslCipherSuites=UNKNOWN_CIPHER"),
        "Unsupported SSL cipher");
  }

  @Test
  public void certificateMandatorySsl() throws Throwable {
    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon(BASE_OPTIONS + "&sslMode=VERIFY_CA"),
        "No X509TrustManager found");

    try (Connection con =
        createCon(BASE_OPTIONS + "&sslMode=VERIFY_CA&serverSslCert=" + SERVER_CERT_PATH)) {
      assertNotNull(getSslVersion(con));
    }

    try (Connection con =
        createCon(BASE_OPTIONS + "&sslMode=VERIFY_CA&serverSslCert=file:///wrongPath")) {
      assertNotNull(getSslVersion(con));
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IOException);
    }

    String urlPath = Paths.get(SERVER_CERT_PATH).toUri().toURL().toString();
    // file certificate path, like  file:/path/certificate.crt
    try (Connection con = createCon(BASE_OPTIONS + "&sslMode=VERIFY_CA&serverSslCert=" + urlPath)) {
      assertNotNull(getSslVersion(con));
    }

    String certificateString = getServerCertificate(SERVER_CERT_PATH);
    // file certificate, like  -----BEGIN CERTIFICATE-----...
    try (Connection con =
        createCon(BASE_OPTIONS + "&sslMode=VERIFY_CA&serverSslCert=" + certificateString)) {
      assertNotNull(getSslVersion(con));
    }

    Common.assertThrowsContains(
        SQLException.class,
        () -> createCon(BASE_OPTIONS + "&sslMode=VERIFY_FULL&serverSslCert=" + urlPath),
        "DNS host \"localhost\" doesn't correspond to certificate CN \"singlestore-server\"");
  }

  @Test
  public void trustStoreMandatorySsl() throws Throwable {
    try (Connection con =
        createCon(
            BASE_OPTIONS
                + "&sslMode=VERIFY_CA&trustStore="
                + TRUST_STORE_PATH
                + "&trustStorePassword="
                + TRUST_STORE_PASSWORD)) {
      assertNotNull(getSslVersion(con));
    }
  }

  @Test
  public void trustStoreParameter() throws Throwable {
    KeyStore ks = KeyStore.getInstance("jks");
    char[] pwdArray = "myPwd0".toCharArray();
    ks.load(null, pwdArray);

    File temptrustStoreFile = File.createTempFile("newKeyStoreFileName", ".jks");

    KeyStore ks2 = KeyStore.getInstance("pkcs12");
    ks2.load(null, pwdArray);
    File temptrustStoreFile2 = File.createTempFile("newKeyStoreFileName", ".pkcs12");

    try (InputStream inStream = new File(SERVER_CERT_PATH).toURI().toURL().openStream()) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Collection<? extends Certificate> serverCertList = cf.generateCertificates(inStream);
      List<Certificate> certs = new ArrayList<>();
      for (Iterator<? extends Certificate> iter = serverCertList.iterator(); iter.hasNext(); ) {
        certs.add(iter.next());
      }
      try (InputStream inStream2 = new File(CA_CERT_PATH).toURI().toURL().openStream()) {
        CertificateFactory cf2 = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> caCertList = cf2.generateCertificates(inStream2);
        for (Iterator<? extends Certificate> iter = caCertList.iterator(); iter.hasNext(); ) {
          certs.add(iter.next());
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
            BASE_OPTIONS
                + "&sslMode=VERIFY_CA&trustStore="
                + temptrustStoreFile
                + "&trustStoreType=jks&trustStorePassword=myPwd0")) {
      assertNotNull(getSslVersion(con));
    }

    // with alias
    try (Connection con =
        createCon(
            BASE_OPTIONS
                + "&sslMode=VERIFY_CA&trustCertificateKeystoreUrl="
                + temptrustStoreFile
                + "&trustCertificateKeyStoretype=jks&trustCertificateKeystorePassword=myPwd0")) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                BASE_OPTIONS
                    + "&sslMode=VERIFY_CA&trustStore="
                    + temptrustStoreFile
                    + "&trustStoreType=jks&trustStorePassword=wrongPwd"),
        "Failed load keyStore");
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                BASE_OPTIONS
                    + "&sslMode=VERIFY_CA&trustCertificateKeystoreUrl="
                    + temptrustStoreFile
                    + "&trustCertificateKeyStoretype=jks&trustCertificateKeystorePassword=wrongPwd"),
        "Failed load keyStore");
    try (Connection con =
        createCon(
            BASE_OPTIONS
                + "&sslMode=VERIFY_CA&trustStore="
                + temptrustStoreFile2
                + "&trustStoreType=pkcs12&trustStorePassword=myPwd0")) {
      assertNotNull(getSslVersion(con));
    }
    assertThrowsContains(
        SQLException.class,
        () ->
            createCon(
                BASE_OPTIONS
                    + "&sslMode=VERIFY_CA&trustStore="
                    + temptrustStoreFile2
                    + "&trustStoreType=pkcs12&trustStorePassword=wrongPwd"),
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
