// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.
package com.singlestore.jdbc.plugin.authentication.standard;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.plugin.AuthenticationPlugin;
import com.singlestore.jdbc.plugin.AuthenticationPluginFactory;

/**
 * PAM (dialog) authentication plugin. This is a multi-step exchange password. If more than one
 * step, passwordX (password2, password3, ...) options must be set.
 */
public class SendPamAuthPacketFactory implements AuthenticationPluginFactory {

  @Override
  public String type() {
    return "dialog";
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
    return new SendPamAuthPacket(authenticationData, conf);
  }
}