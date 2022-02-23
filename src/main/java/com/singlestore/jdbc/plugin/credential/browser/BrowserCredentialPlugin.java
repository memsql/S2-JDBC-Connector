// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.credential.Credential;
import com.singlestore.jdbc.plugin.credential.CredentialPlugin;
import com.singlestore.jdbc.plugin.credential.browser.keyring.Keyring;
import java.sql.SQLException;

public class BrowserCredentialPlugin implements CredentialPlugin {

  private BrowserCredentialGenerator generator;
  private final Keyring keyring;

  private String userEmail;
  private ExpiringCredential credential;

  @Override
  public String type() {
    return "BROWSER";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  @Override
  public String defaultAuthenticationPluginType() {
    return "mysql_clear_password";
  }

  public BrowserCredentialPlugin() {
    this.keyring = Keyring.buildForCurrentOS();
  }

  @Override
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress)
      throws SQLException {
    this.generator = new BrowserCredentialGenerator(conf.nonMappedOptions());

    return this;
  }

  @Override
  // get() is synchronized to avoid requesting credentials twice if a second thread tries to
  // establish a connection while user sign-in is still in progress
  public synchronized Credential get() throws SQLException {
    if (credential != null && credential.isValid()) {
      return credential.getCredential();
    }

    ExpiringCredential cred = null;
    if (keyring != null) {
      cred = keyring.getCredential();
    }

    if (cred == null || !cred.isValid()) {
      cred = generator.getCredential(userEmail);

      if (keyring != null) {
        keyring.setCredential(cred);
      }
    }

    credential = cred;
    userEmail = cred.getEmail();
    return cred.getCredential();
  }

  // this function is for testing only
  public void clear() {
    userEmail = null;
    credential = null;
  }
}
