// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.plugin.authentication.addon;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.plugin.AuthenticationPlugin;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClearPasswordPlugin implements AuthenticationPlugin {

  private String authenticationData;

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   */
  public ClearPasswordPlugin(String authenticationData) {
    this.authenticationData = authenticationData;
  }

  /**
   * Send password in clear text to server.
   *
   * @param out out stream
   * @param in in stream
   * @param context context
   * @return response packet
   * @throws IOException if socket error
   */
  @Override
  public ReadableByteBuf process(Writer out, Reader in, Context context) throws IOException {
    if (authenticationData == null) {
      out.writeEmptyPacket();
    } else {
      byte[] bytePwd = authenticationData.getBytes(StandardCharsets.UTF_8);
      out.writeBytes(bytePwd);
      out.writeByte(0);
      out.flush();
    }

    return in.readReusablePacket();
  }
}
