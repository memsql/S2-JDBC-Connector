// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.plugin.authentication.addon.gssapi;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import com.sun.jna.platform.win32.Sspi;
import com.sun.jna.platform.win32.SspiUtil;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.ietf.jgss.GSSCredential;
import waffle.windows.auth.IWindowsSecurityContext;
import waffle.windows.auth.impl.WindowsSecurityContextImpl;

public class WindowsNativeSspiAuthentication implements GssapiAuth {

  private static final Logger logger = Loggers.getLogger(WindowsNativeSspiAuthentication.class);

  /**
   * Process native windows GSS plugin authentication.
   *
   * @param out out stream
   * @param in in stream
   * @param servicePrincipalName principal name
   * @param jaasApplicationName entry name in JAAS Login Configuration File
   * @param gssCredential pre-obtained GSS credential (not supported on this path)
   * @param requestCredentialDelegation request Kerberos delegation (not exposed by Waffle {@link
   *     WindowsSecurityContextImpl#getCurrent})
   * @param mechanisms gssapi mechanism
   * @throws SQLFeatureNotSupportedException if {@code gssCredential} is non-null on this SSPI path
   * @throws IOException if socket error
   */
  public void authenticate(
      final Context ctx,
      final Writer out,
      final Reader in,
      final String servicePrincipalName,
      final String jaasApplicationName,
      final GSSCredential gssCredential,
      final boolean requestCredentialDelegation,
      final String mechanisms)
      throws IOException, SQLException {

    if (gssCredential != null) {
      throw new SQLFeatureNotSupportedException(
          "GSSCredential passthrough is not supported with Windows native SSPI authentication. "
              + "Use the JGSS (Kerberos) code path: run on non-Windows or remove Waffle from the classpath.");
    }

    // WindowsSecurityContextImpl.getCurrent(protocol, target) does not expose ISC_REQ_DELEGATE
    if (requestCredentialDelegation) {
      logger.debug("requestCredentialDelegation is ignored on Windows native SSPI (Waffle) path");
    }

    // initialize a security context on the client
    IWindowsSecurityContext clientContext =
        WindowsSecurityContextImpl.getCurrent(mechanisms, servicePrincipalName);

    do {

      // Step 1: send token to server
      byte[] tokenForTheServerOnTheClient = clientContext.getToken();
      if (tokenForTheServerOnTheClient != null && tokenForTheServerOnTheClient.length > 0) {
        out.writeBytes(tokenForTheServerOnTheClient);
        out.flush();
      }
      if (!clientContext.isContinue()) {
        break;
      }

      // Step 2: read server response token
      ReadableByteBuf buf = in.readReusablePacket();
      byte[] tokenForTheClientOnTheServer = new byte[buf.readableBytes()];
      buf.readBytes(tokenForTheClientOnTheServer);
      Sspi.SecBufferDesc continueToken =
          new SspiUtil.ManagedSecBufferDesc(Sspi.SECBUFFER_TOKEN, tokenForTheClientOnTheServer);
      clientContext.initialize(clientContext.getHandle(), continueToken, servicePrincipalName);

    } while (true);

    clientContext.dispose();
  }
}
