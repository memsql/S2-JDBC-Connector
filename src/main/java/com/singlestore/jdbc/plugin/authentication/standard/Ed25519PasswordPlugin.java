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
import com.singlestore.jdbc.plugin.authentication.standard.ed25519.math.GroupElement;
import com.singlestore.jdbc.plugin.authentication.standard.ed25519.math.ed25519.Ed25519ScalarOps;
import com.singlestore.jdbc.plugin.authentication.standard.ed25519.spec.EdDSANamedCurveTable;
import com.singlestore.jdbc.plugin.authentication.standard.ed25519.spec.EdDSAParameterSpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;

// not used in SingleStore
public class Ed25519PasswordPlugin implements AuthenticationPlugin {

  private final String authenticationData;
  private final byte[] seed;

  /**
   * Initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection string options
   */
  public Ed25519PasswordPlugin(String authenticationData, byte[] seed, Configuration conf) {
    this.seed = seed;
    this.authenticationData = authenticationData;
  }

  /**
   * Sign password
   *
   * @param password password
   * @param seed server seed
   * @return encrypted value
   * @throws SQLException if any error occurs
   */
  private static byte[] ed25519SignWithPassword(final String password, final byte[] seed)
      throws SQLException {

    try {
      byte[] bytePwd = password.getBytes(StandardCharsets.UTF_8);

      MessageDigest hash = MessageDigest.getInstance("SHA-512");

      int mlen = seed.length;
      final byte[] sm = new byte[64 + mlen];

      byte[] az = hash.digest(bytePwd);
      az[0] &= (byte) 248;
      az[31] &= 63;
      az[31] |= 64;

      System.arraycopy(seed, 0, sm, 64, mlen);
      System.arraycopy(az, 32, sm, 32, 32);

      byte[] buff = Arrays.copyOfRange(sm, 32, 96);
      hash.reset();
      byte[] nonce = hash.digest(buff);

      Ed25519ScalarOps scalar = new Ed25519ScalarOps();

      EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName("Ed25519");
      GroupElement elementAvalue = spec.getB().scalarMultiply(az);
      byte[] elementAarray = elementAvalue.toByteArray();
      System.arraycopy(elementAarray, 0, sm, 32, elementAarray.length);

      nonce = scalar.reduce(nonce);
      GroupElement elementRvalue = spec.getB().scalarMultiply(nonce);
      byte[] elementRarray = elementRvalue.toByteArray();
      System.arraycopy(elementRarray, 0, sm, 0, elementRarray.length);

      hash.reset();
      byte[] hram = hash.digest(sm);
      hram = scalar.reduce(hram);
      byte[] tt = scalar.multiplyAndAdd(hram, az, nonce);
      System.arraycopy(tt, 0, sm, 32, tt.length);

      return Arrays.copyOfRange(sm, 0, 64);

    } catch (NoSuchAlgorithmException e) {
      throw new SQLException("Could not use SHA-512, failing", e);
    }
  }

  /**
   * Process Ed25519 password plugin authentication. see
   * https://mariadb.com/kb/en/library/authentication-plugin-ed25519/
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
    if (authenticationData == null) {
      out.writeEmptyPacket();
    } else {
      out.writeBytes(ed25519SignWithPassword(authenticationData, seed));
      out.flush();
    }

    return in.readReusablePacket();
  }
}
