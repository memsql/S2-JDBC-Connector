package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.plugin.credential.CredentialPluginLoader;
import com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialPlugin;
import java.io.*;
import java.nio.file.StandardCopyOption;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BrowserAuthTest extends Common {
  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();

    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS SPCM1DAADM3DF001");
    stmt.execute("CREATE USER SPCM1DAADM3DF001 IDENTIFIED WITH authentication_jwt");
    stmt.execute("GRANT ALL PRIVILEGES ON test.* TO SPCM1DAADM3DF001");
  }

  @Test
  public void mockAuthHelper() throws SQLException, IOException {
    File mockAuthHelper = setupMockAuthHelper();

    String connString =
        String.format("jdbc:singlestore://%s:%s/", hostname, port)
            + sharedConn.getCatalog()
            + "?credentialType=BROWSER&authHelperPath="
            + mockAuthHelper;

    java.sql.Connection connection = DriverManager.getConnection(connString + "&sslMode=trust");
    ResultSet rs = connection.createStatement().executeQuery("select 1");
    assertTrue(rs.next());

    assertThrowsContains(
        Exception.class,
        () -> DriverManager.getConnection(connString),
        "Cannot send password in clear text if SSL is not enabled.");

    assertTrue(mockAuthHelper.delete());
  }

  @Test
  public void useKeystore() throws SQLException, IOException {
    File mockAuthHelper = setupMockAuthHelper();
    String connString =
        String.format("jdbc:singlestore://%s:%s/", hostname, port)
            + sharedConn.getCatalog()
            + "?credentialType=BROWSER&authHelperPath="
            + mockAuthHelper
            + "&sslMode=trust";

    try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
      ResultSet rs = connection.createStatement().executeQuery("select 1");
      assertTrue(rs.next());
    }

    // remove cached credentials from the plugin
    BrowserCredentialPlugin credPlugin =
        (BrowserCredentialPlugin) CredentialPluginLoader.get("BROWSER");
    credPlugin.clear();

    // remove the contents of mockAuthHelper so it cannot be run properly.
    // The token must be cached in the keystore
    new PrintWriter(mockAuthHelper.getPath()).close();

    try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
      ResultSet rs = connection.createStatement().executeQuery("select 1");
      assertTrue(rs.next());
    }
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS SPCM1DAADM3DF001");
  }

  private File setupMockAuthHelper() throws IOException {
    File mockAuthHelper;
    try (InputStream inStream =
        BrowserAuthTest.class.getClassLoader().getResourceAsStream("mockAuthHelper.sh")) {
      assertNotNull(inStream);
      mockAuthHelper = File.createTempFile("mock-auth-", "");
      java.nio.file.Files.copy(
          inStream, mockAuthHelper.toPath(), StandardCopyOption.REPLACE_EXISTING);
      assertTrue(mockAuthHelper.setExecutable(true));
      return mockAuthHelper;
    }
  }
}
