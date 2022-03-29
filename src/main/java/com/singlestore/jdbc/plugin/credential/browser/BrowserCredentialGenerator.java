package com.singlestore.jdbc.plugin.credential.browser;

import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.sql.SQLException;
import java.util.Properties;

public class BrowserCredentialGenerator {
  private static final Logger logger = Loggers.getLogger(BrowserCredentialGenerator.class);

  public BrowserCredentialGenerator(Properties nonMappedOptions) throws SQLException {}

  public ExpiringCredential getCredential(String email) throws SQLException {
    TokenWaiterServer server = new TokenWaiterServer();
    String listenPath = server.getListenPath();
    logger.debug("Listening on " + listenPath);

    try {
      return server.WaitForCredential();
    } catch (InterruptedException e) {
      throw new SQLException("Interrupted while waiting for JWT", e);
    }
  }
}
