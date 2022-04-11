package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.plugin.credential.CredentialPluginLoader;
import com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialPlugin;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BrowserAuthTest extends Common {
  /*
  Keys used to sign the JWTs used in these tests. The public key duplicated in
  scripts/jwt/jwt_auth_config.json which is used to tell SingleStore to accept the tokens

  -----BEGIN RSA PRIVATE KEY-----
  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
  -----END RSA PRIVATE KEY-----

  -----BEGIN PUBLIC KEY-----
  MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxSS8EPCSBiti+RFAK9A+
  y9zZfiaf97QZUwDKxoWOQoPP4lK0o41pPXfTyOIFst1AW8aNZ0ulqNu6s/LbJ2X8
  FLli4giSOOmRkXq31lXe1fheAMJ43S2rwHBJJqihMXXBTXbVOFu2nJP68myPw7ZM
  Td4Y2EsjSrgeAhdS17QKj5vZQ6xvCrF8Fa523Zyz1+TXgvYEIuhC1uorT0+P5VuT
  8SRC4DC0n6yU3x+8Xkx6R9BWzG8wgZIL3WTaFjtZN5TJ6JS+5sJWzWRPyJUYaGnA
  H5362BxIox53WnN0DnKNrZ3wAxxmRd2+3D33UEHGNDl5WQeW2Ov2AL+Zpqqeasz7
  Emhf8rSZBw8w2X1DMigrSbdadL6fanC0brltZx6XlqGFBvBTc6wt1ZZaDDz70lSZ
  qsh1CkdWf9hxlq5d19Q9Onguo5y3uJHok1ixGXWgOQq4aSKRf775vN/iQtsUWb8P
  OtPp+LXsJ6GCE7TZUIrpQmsAF8j5lxLTDFFyjJUirSaHRJqwUOGoJnkgCqGbm9rF
  aG67JWaTaFmWaQeW17Lc5xqCscuSvrw5K8XTVgiHhFFNnay8LO/iiBjYtpz8kKbX
  fXDt3LARfBbnz4xrtg6OEF+jz9bFSdUeHSf7ql2SArW20W+1pTGqaLw8hAb7mbZ5
  tXuINPufsprQ8/I9PjNXMV0CAwEAAQ==
  -----END PUBLIC KEY-----
   */
  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();

    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS jwt_user");
    stmt.execute("CREATE USER jwt_user IDENTIFIED WITH authentication_jwt");
    stmt.execute("GRANT ALL PRIVILEGES ON test.* TO jwt_user");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS SPCM1DAADM3DF001");
  }

  @Test
  public void mockBrowser() throws IOException, SQLException {
    /*{
      "email": "test-email@gmail.com",
      "dbUsername": "jwt_user",
      "exp": 1916239022 (year 2030)
    }*/
    String jwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwiZGJVc2VybmFt"
            + "ZSI6Imp3dF91c2VyIiwiZXhwIjoxOTE2MjM5MDIyfQ.Bo_LcWJrzflkSvVFuJglUzdJPHVsQFZ0JTu0a0zK4J60Bhfed-PCk7I"
            + "o7lcvmflWDdl9j6ZzbdZOfyAoywg2ME8DGlbgv29Xy1h0BCpnDFhaOl_TVTc40pI_IqCrn97D53pgXH31-KQu5F0ap26j0DwVM"
            + "0Zk22rYeOFeBOFWSy_HcBoC6UQ9HaZaXEY1aaCP_fOUwMN7HGGJ3vR0VCn7UPLAT3wibeH0b9PspVyqQ2fs3cNAqJo9_sYWBAz"
            + "-B4gsQCMXsBCpUV_Rn4r1RZI7cDsjsKHcoVleLD-oS4z8zzo472qYd9DWwciVRutTUgOC9Z7LekxUY9RHhvuUmBNbvBKI8qrJQ"
            + "Scj6wWmmmkRlgT4PYYfmRpmOwxr7Y9M4rr_9F1bOtK1Pf6ml9NCHTW3agF9VO3tvtvlUgnlIeZeAECg6UvtGyxwQmDLNdv6EO1"
            + "CrP49wbtZSI8O04z-yCCVE-XPPpW0iTAZmGFOu8cDsCxTOnxjKrN7hEHkU4g8hV7NwVHgHyaM5PS06DL0RN2VWXxvbbOVZqc-J"
            + "sURR8H8vTxVQoxBcUXx9o23FjfdIYa5iFEb8_mdkhWU6CPSKVqE0zXgoO8yyUiXN2aF0-xxY2wptruQnbpkVE3cUNPuUTG9WlH"
            + "7e1x61-gZISLl-43Bz04Nfqw5C8YvYVjm22KZq9o";
    MockHttpServer ssoServer = new MockHttpServer(jwt, false, 1, 0);
    try {
      // make sure no creds are cached
      BrowserCredentialPlugin credPlugin =
          (BrowserCredentialPlugin) CredentialPluginLoader.get("BROWSER_SSO");
      credPlugin.clearKeyring();
      credPlugin.clearLocalCache();

      String connString =
          String.format("jdbc:singlestore://%s:%s/", hostname, port)
              + sharedConn.getCatalog()
              + "?credentialType=BROWSER_SSO"
              + "&sslMode=trust";

      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }

      // remove creds from in-memory storage, they should be stored in the keyring
      credPlugin.clearLocalCache();

      // should not query for token again (verified by the MockHttpServer)
      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }

      // this time remove creds from the keyring, they should be stored in-memory
      credPlugin.clearKeyring();

      // should not query for token again (verified by the MockHttpServer)
      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }
    } finally {
      ssoServer.stop();
    }
  }

  @Test
  public void mockBrowserError() throws IOException, SQLException {
    /*{
      "email": "test-email@gmail.com",
      "dbUsername": "wrong_user",
      "exp": 1916239022 (year 2030)
    }*/
    String jwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwiZGJVc2VybmFtZSI6In"
            + "dyb25nX3VzZXIiLCJleHAiOjE5MTYyMzkwMjJ9.xM_14g4eBvymw8wtjC_TzBn45GrBga_VVlPGQh-0y7aSMVQ77RGG"
            + "c0BxoYeNMu885MH7SK20lP3NiWxQzZblrbPa86ueeApNEG4ndZLkP_7zgpcRtUBu9KQWOmMYiEW8_r2CVz1wxQh36SN"
            + "S8BlvQRXXVhYwpWfbWX4lAcI0BRK04iRGwUuJnRWXVfXreELlErHn_Nu8GxU3GWl0nkFUtgdc1mt8ic580TQNo1o3IM"
            + "oMbzPErDxKRtDNPRbO38hmawaskYWhbUCmyH-LTb8-p9cBUPfIT5rgiagr9qRLu9JtZ3bQc2PI979Cyj1RfriG5NMs7"
            + "uf3DnjNYhIkEqvCZ-fpJ2VfcHs3TGFcy1ygQRlxYNhenwUPRiKigL6nHx7ksJaVqFIjQq2soZnuwm-gJuERF5L4HwLq"
            + "yIYmKnxxcfPdMLCv4f4CGfZji17CxP4KxQzRz-8k5lj_-ZFV1uZpdhPl6-nUA-dMdns7QV6vuNPa4jMP3rl5wBQaV-y"
            + "4iOV98tcgInrdw0PmLZeuUPvOcfw0wQJiQY1K84imnfTRJhfaLSvZcF6KhxQxn0d1q9ibsJL1NWBWPYhrUtAfDJEm2r"
            + "fe89CVJEnUIpuHbjNKBVeYrgx-Hgvau-lsPK3MDNJUAm1R8d0xG4t3UjElk8pZBudeKvO7TyKXbZNJ6sIvqqg";
    MockHttpServer ssoServer = new MockHttpServer(jwt, false, 2, 0);

    try {
      // make sure no creds are cached in the keyring
      BrowserCredentialPlugin credPlugin =
          (BrowserCredentialPlugin) CredentialPluginLoader.get("BROWSER_SSO");
      credPlugin.clearKeyring();

      String connString =
          String.format("jdbc:singlestore://%s:%s/", hostname, port)
              + sharedConn.getCatalog()
              + "?credentialType=BROWSER_SSO"
              + "&sslMode=trust";

      assertThrowsContains(
          SQLException.class,
          () -> DriverManager.getConnection(connString),
          "Access denied for user 'wrong_user'@'172.17.0.1'");
    } finally {
      ssoServer.stop();
    }
  }

  private static class MockHttpServer {
    private final HttpServer server;
    private int packetsLeft;

    public MockHttpServer(
        String jwt, boolean shouldHaveEmail, int expectedPackets, int shouldReceiveErrorWithCode)
        throws IOException {
      server = HttpServer.create(new InetSocketAddress(18087), 0);
      packetsLeft = expectedPackets;

      String path = "/";
      server.createContext(
          path,
          exchange -> {
            try {
              if (packetsLeft <= 0) {
                throw new IOException("Got a second packet");
              }
              packetsLeft -= 1;

              Map<String, String> params =
                  URLEncodedUtils.parse(exchange.getRequestURI(), StandardCharsets.UTF_8).stream()
                      .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
              assertNotNull(params.get("returnTo"));
              if (shouldHaveEmail) {
                assertNotNull(params.get("email"));
              }

              HttpClient httpclient = HttpClients.createDefault();
              HttpPost httppost = new HttpPost(params.get("returnTo"));
              StringEntity entity = new StringEntity(jwt);
              httppost.setEntity(entity);
              HttpResponse response = httpclient.execute(httppost);

              assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");
              if (shouldReceiveErrorWithCode != 0) {
                assertEquals(response.getStatusLine().getStatusCode(), shouldReceiveErrorWithCode);
                assertNotNull(response.getEntity());
              } else {
                assertEquals(response.getStatusLine().getStatusCode(), 204);
                assertNull(response.getEntity());
              }
              exchange.sendResponseHeaders(204, -1);
            } catch (Exception e) {
              exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
              exchange.sendResponseHeaders(400, 0);
              exchange.getResponseBody().write(e.getMessage().getBytes(StandardCharsets.UTF_8));
              exchange.getResponseBody().close();
            } finally {
              exchange.close();
            }
          });
      server.start();
    }

    public void stop() {
      server.stop(0);
    }
  }
}
