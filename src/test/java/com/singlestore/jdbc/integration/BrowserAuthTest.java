package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Statement;
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
    stmt.execute("GRANT SELECT ON *.* TO 'browserAuthUser' IDENTIFIED BY 'mock_token'");
  }

  @Test
  public void mockAuthHelper() throws SQLException, IOException {
    File mockAuthHelper = setupMockAuthHelper();

    java.sql.Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:singlestore://%s:%s/", hostname, port)
                + sharedConn.getCatalog()
                + "?credentialType=BROWSER&authHelperPath="
                + mockAuthHelper);
    ResultSet rs = connection.createStatement().executeQuery("select 1");
    assertTrue(rs.next());

    assertTrue(mockAuthHelper.delete());
  }

  @Test
  public void useKeystore() throws SQLException, IOException {
    File mockAuthHelper = setupMockAuthHelper();

    try (java.sql.Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:singlestore://%s:%s/", hostname, port)
                + sharedConn.getCatalog()
                + "?credentialType=BROWSER&authHelperPath="
                + mockAuthHelper)) {
      ResultSet rs = connection.createStatement().executeQuery("select 1");
      assertTrue(rs.next());
    }

    // removes the contents of mockAuthHelper so it cannot be run properly.
    // The token must be cached in the keystore
    new PrintWriter(mockAuthHelper.getPath()).close();

    try (java.sql.Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:singlestore://%s:%s/", hostname, port)
                + sharedConn.getCatalog()
                + "?credentialType=BROWSER&authHelperPath="
                + mockAuthHelper)) {
      ResultSet rs = connection.createStatement().executeQuery("select 1");
      assertTrue(rs.next());
    }
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS browserAuthUser");
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
