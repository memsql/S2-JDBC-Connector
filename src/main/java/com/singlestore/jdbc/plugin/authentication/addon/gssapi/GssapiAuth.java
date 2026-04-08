// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.plugin.authentication.addon.gssapi;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.client.socket.Writer;
import java.io.IOException;
import java.sql.SQLException;
import org.ietf.jgss.GSSCredential;

public interface GssapiAuth {

  /**
   * Authenticate
   *
   * @param writer socket writer
   * @param in socket reader
   * @param servicePrincipalName SPN
   * @param jaasApplicationName application name
   * @param gssCredential optional pre-obtained credential; when non-null, JAAS login is skipped
   * @param requestCredentialDelegation when true, request GSS credential delegation
   * @param mechanisms mechanisms
   * @throws IOException if any socket error occurs
   * @throws SQLException for any other type of errors
   */
  void authenticate(
      Context ctx,
      Writer writer,
      Reader in,
      String servicePrincipalName,
      String jaasApplicationName,
      GSSCredential gssCredential,
      boolean requestCredentialDelegation,
      String mechanisms)
      throws SQLException, IOException;
}
