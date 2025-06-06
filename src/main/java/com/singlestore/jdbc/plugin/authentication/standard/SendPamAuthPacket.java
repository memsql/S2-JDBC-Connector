// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.plugin.authentication.standard;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.plugin.AuthenticationPlugin;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class SendPamAuthPacket implements AuthenticationPlugin {

  private final String authenticationData;
  private final Configuration conf;
  private int counter = 0;

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param conf Connection string options
   */
  public SendPamAuthPacket(String authenticationData, Configuration conf) {
    this.authenticationData = authenticationData;
    this.conf = conf;
  }

  /**
   * Process PAM plugin authentication. see
   * https://docs.singlestore.com/db/v7.5/en/security/authentication/using-singlestore-db-and-pam.html
   *
   * @param out out stream
   * @param in in stream
   * @param context connection context
   * @return response packet
   * @throws IOException if socket error
   */
  @Override
  public ReadableByteBuf process(Writer out, Reader in, Context context)
      throws SQLException, IOException {

    while (true) {
      counter++;
      String password;
      if (counter == 1) {
        password = authenticationData;
      } else {
        if (!conf.nonMappedOptions().containsKey("password" + counter)) {
          throw new SQLException(
              "PAM authentication request multiple passwords, but "
                  + "'password"
                  + counter
                  + "' is not set");
        }
        password = (String) conf.nonMappedOptions().get("password" + counter);
      }

      byte[] bytePwd = password != null ? password.getBytes(StandardCharsets.UTF_8) : new byte[0];
      out.writeBytes(bytePwd, 0, bytePwd.length);
      out.writeByte(0);
      out.flush();

      ReadableByteBuf buf = in.readReusablePacket();

      int type = buf.getUnsignedByte();

      // PAM continue until finish.
      if (type == 0xfe // Switch Request
          || type == 0x00 // OK_Packet
          || type == 0xff) { // ERR_Packet
        return buf;
      }
    }
  }
}
