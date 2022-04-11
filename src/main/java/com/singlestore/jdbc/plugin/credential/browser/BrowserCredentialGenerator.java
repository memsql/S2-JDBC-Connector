package com.singlestore.jdbc.plugin.credential.browser;

import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;

public class BrowserCredentialGenerator {
  private static final Logger logger = Loggers.getLogger(BrowserCredentialGenerator.class);
  protected String baseURL;

  public BrowserCredentialGenerator(String baseURL) {
    this.baseURL = baseURL;
  }

  public ExpiringCredential getCredential(String email) throws SQLException {
    TokenWaiterServer server = new TokenWaiterServer();
    String listenPath = server.getListenPath();
    logger.debug("Listening on " + listenPath);

    String urlParams = "returnTo=" + listenPath;
    if (email != null) {
      urlParams += "&email=" + email;
    }

    URL fullURL;
    try {
      URL base = new URL(baseURL);
      // do this to properly encode the URL
      fullURL =
          new URI(base.getProtocol(), base.getAuthority(), base.getPath(), urlParams, null).toURL();
    } catch (MalformedURLException | URISyntaxException e) {
      throw new SQLException("Failed to build a URL while using BROSWER-SSO identity plugin", e);
    }

    openBrowser(fullURL.toString());

    try {
      return server.WaitForCredential();
    } catch (InterruptedException e) {
      throw new SQLException("Interrupted while waiting for JWT", e);
    }
  }

  protected void openBrowser(String url) throws SQLException {
    Runtime rt = Runtime.getRuntime();
    String operSys = System.getProperty("os.name").toLowerCase();
    try {
      if (operSys.contains("win")) {
        rt.exec("rundll32 url.dll,FileProtocolHandler " + url);
      } else if (operSys.contains("nix") || operSys.contains("nux") || operSys.contains("aix")) {
        rt.exec("xdg-open " + url);
      } else if (operSys.contains("mac")) {
        rt.exec("open " + url);
      }
    } catch (IOException e) {
      throw new SQLException("Failed to open a browser while using BROSWER-SSO identity plugin", e);
    }
  }
}
