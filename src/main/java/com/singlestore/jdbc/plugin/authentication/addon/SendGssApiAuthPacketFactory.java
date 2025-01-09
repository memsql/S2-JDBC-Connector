// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.plugin.authentication.addon;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.plugin.AuthenticationPlugin;
import com.singlestore.jdbc.plugin.AuthenticationPluginFactory;

/** GSSAPI plugin */
public class SendGssApiAuthPacketFactory implements AuthenticationPluginFactory {

  @Override
  public String type() {
    return "auth_gssapi_client";
  }

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection string options
   */
  public AuthenticationPlugin initialize(
      String authenticationData, byte[] seed, Configuration conf) {
    return new SendGssApiAuthPacket(seed, conf);
  }
}
