// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 SingleStore, Inc.

package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Configuration;
import java.sql.SQLException;
import java.util.Properties;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for GSS Kerberos delegation-related connection properties. Does not extend {@link
 * com.singlestore.jdbc.integration.Common} so it runs without a live database.
 */
public class ConfigurationGssDelegationTest {

  @Test
  public void requestCredentialDelegationFromUrl() throws SQLException {
    Configuration conf =
        Configuration.parse("jdbc:singlestore://localhost/test?requestCredentialDelegation=true");
    assertTrue(Boolean.TRUE.equals(conf.requestCredentialDelegation()));
  }

  @Test
  public void requestCredentialDelegationDefaultNull() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test");
    assertNull(conf.requestCredentialDelegation());
  }

  @Test
  public void gssCredentialWrongTypeThrows() {
    Properties props = new Properties();
    props.put("gssCredential", "not-a-credential");
    SQLException ex =
        assertThrows(
            SQLException.class,
            () -> Configuration.parse("jdbc:singlestore://localhost/test", props));
    assertTrue(ex.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void gssCredentialFromProperties() throws SQLException {
    StubGssCredential cred = new StubGssCredential();
    Properties props = new Properties();
    props.put("gssCredential", cred);
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test", props);
    assertEquals(cred, conf.gssCredential());
  }

  /** Minimal {@link GSSCredential} for configuration property tests only. */
  private static final class StubGssCredential implements GSSCredential {
    @Override
    public void dispose() {}

    @Override
    public GSSName getName() {
      return null;
    }

    @Override
    public GSSName getName(Oid mech) {
      return null;
    }

    @Override
    public int getRemainingLifetime() {
      return 0;
    }

    @Override
    public int getRemainingInitLifetime(Oid mech) {
      return 0;
    }

    @Override
    public int getRemainingAcceptLifetime(Oid mech) {
      return 0;
    }

    @Override
    public int getUsage() {
      return INITIATE_ONLY;
    }

    @Override
    public int getUsage(Oid mech) {
      return INITIATE_ONLY;
    }

    @Override
    public Oid[] getMechs() {
      return new Oid[0];
    }

    @Override
    public void add(GSSName name, int initLifetime, int acceptLifetime, Oid mech, int usage)
        throws GSSException {}
  }
}
