// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.
package com.singlestore.jdbc.plugin.authentication.standard;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.plugin.AuthenticationPlugin;
import com.singlestore.jdbc.plugin.AuthenticationPluginFactory;

/** ED25519 password plugin */
public class Ed25519PasswordPluginFactory implements AuthenticationPluginFactory {

  @Override
  public String type() {
    return "client_ed25519";
  }

  public AuthenticationPlugin initialize(
      String authenticationData, byte[] seed, Configuration conf) {
    return new Ed25519PasswordPlugin(authenticationData, seed, conf);
  }
}
