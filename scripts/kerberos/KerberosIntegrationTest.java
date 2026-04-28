/*
 * ************************************************************************************
 *   Copyright (c) 2026 SingleStore, Inc.
 *
 *   This library is free software; you can redistribute it and/or
 *   modify it under the terms of the GNU Library General Public
 *   License as published by the Free Software Foundation; either
 *   version 2.1 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *   Library General Public License for more details.
 *
 *   You should have received a copy of the GNU Library General Public
 *   License along with this library; if not see <http://www.gnu.org/licenses>
 *   or write to the Free Software Foundation, Inc.,
 *   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 * *************************************************************************************/

import com.sun.security.jgss.ExtendedGSSCredential;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

/**
 * Single-entrypoint Kerberos integration test harness driven by environment
 * variables.
 *
 * Usage:
 *   java KerberosIntegrationTest
 *
 * Environment variables:
 *   KRB_JDBC_URL             (required) full JDBC URL including kerberos-related
 *                                       query parameters (servicePrincipalName,
 *                                       jaasApplicationName, cacheJaasLoginContext,
 *                                       requestCredentialDelegation, ...)
 *   KRB_USER                 (required) kerberos principal short name to connect as
 *   KRB_USE_GSS_CREDENTIAL   (optional, default false) if true, pre-obtain a
 *                                       GSSCredential via JAAS + Kerberos
 *                                       constrained delegation (S4U2Self +
 *                                       S4U2Proxy) and pass it to the driver
 *                                       through connection Properties. Requires
 *                                       KRB_IMPERSONATE_AS to be set; the JDBC
 *                                       connection is always made under the
 *                                       impersonated user's identity.
 *   KRB_IMPERSONATE_AS       (required when KRB_USE_GSS_CREDENTIAL=true) full
 *                                       Kerberos principal (e.g.
 *                                       impersonated_user@S2.TEST) of the user
 *                                       the middle-tier principal should
 *                                       impersonate.
 *   KRB_CONNECTION_ATTEMPTS  (optional, default 1) number of sequential connection
 *                                       attempts to execute (useful for exercising
 *                                       cacheJaasLoginContext)
 *   KRB_EXPECT_FAILURE       (optional, default false) if true, a SQLException
 *                                       during connect is treated as the success
 *                                       condition (negative test)
 */
public class KerberosIntegrationTest {

    public static void main(String[] args) throws Exception {
        String url = requireEnv("KRB_JDBC_URL");
        String user = requireEnv("KRB_USER");
        boolean useGssCredential = optBool("KRB_USE_GSS_CREDENTIAL", false);
        int connectionAttempts = optInt("KRB_CONNECTION_ATTEMPTS", 1);
        boolean expectFailure = optBool("KRB_EXPECT_FAILURE", false);

        System.out.println("--- Kerberos integration test ---");
        System.out.println("jdbcUrl            : " + url);
        System.out.println("user               : " + user);
        System.out.println("useGssCredential   : " + useGssCredential);
        System.out.println("connectionAttempts : " + connectionAttempts);
        System.out.println("expectFailure      : " + expectFailure);

        try {
            if (useGssCredential) {
                runWithGssCredential(url, user);
            } else {
                for (int i = 1; i <= connectionAttempts; i++) {
                    System.out.println("Connection attempt #" + i);
                    runBasic(url, user);
                }
            }
        } catch (SQLException ex) {
            if (expectFailure) {
                System.out.println("Expected SQLException: " + ex.getMessage());
                System.out.println("NEGATIVE TEST PASSED");
                return;
            }
            throw ex;
        }

        if (expectFailure) {
            System.err.println("FAIL: connection unexpectedly succeeded");
            System.exit(2);
        }
        System.out.println("ALL TESTS PASSED");
    }

    private static void runBasic(String url, String user) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, null)) {
            System.out.println("SUCCESS: Connected via Kerberos");
            System.out.println("Connection class: " + conn.getClass().getName());

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT 'kerberos-auth-ok' AS status, current_user() AS user_name")) {
                while (rs.next()) {
                    System.out.println("Query result: status=" + rs.getString("status")
                        + ", user=" + rs.getString("user_name"));
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1 + 1 AS result")) {
                rs.next();
                int result = rs.getInt("result");
                if (result != 2) {
                    throw new RuntimeException("Expected 2, got " + result);
                }
                System.out.println("Arithmetic check passed: 1+1=" + result);
            }
        }
    }

    private static void runWithGssCredential(String url, String user) throws Exception {
        String impersonateAs = requireEnv("KRB_IMPERSONATE_AS");

        LoginContext lc = new LoginContext("Krb5ConnectorContext");
        lc.login();
        Subject subject = lc.getSubject();
        System.out.println("JAAS login OK, subject principals: " + subject.getPrincipals());
        System.out.println("Constrained delegation: impersonating " + impersonateAs);

        // Obtain the middle-tier's own INITIATE credential from the current
        // subject, then use the S4U2Self extension exposed through
        // ExtendedGSSCredential.impersonate() to mint a credential for the
        // delegated user. The subsequent initSecContext() against the
        // SingleStore SPN will trigger S4U2Proxy (constrained delegation) at
        // the KDC.
        GSSCredential cred = Subject.callAs(subject, () -> {
            GSSManager mgr = GSSManager.getInstance();
            GSSCredential selfCred = mgr.createCredential(GSSCredential.INITIATE_ONLY);
            GSSName targetName = mgr.createName(impersonateAs, null);
            return ((ExtendedGSSCredential) selfCred).impersonate(targetName);
        });
        System.out.println("Obtained GSSCredential for: " + cred.getName());

        Properties props = new Properties();
        props.put("user", user);
        props.put("gssCredential", cred);

        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("SUCCESS: Connected via pre-obtained GSSCredential");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT current_user() AS u")) {
                rs.next();
                String actualUser = rs.getString("u");
                System.out.println("current_user(): " + actualUser);
                // SingleStore returns current_user() as "name@host_pattern"
                // (e.g. "impersonated_user@%"); match the short name prefix.
                String expected = user;
                int at = expected.indexOf('@');
                if (at >= 0) {
                    expected = expected.substring(0, at);
                }
                if (actualUser == null
                    || !actualUser.toLowerCase().startsWith(expected.toLowerCase() + "@")) {
                    throw new RuntimeException(
                        "Constrained delegation check failed: expected connection "
                            + "to be authenticated as '" + expected
                            + "' but current_user() returned '" + actualUser + "'");
                }
                System.out.println("Constrained delegation verified: connected as "
                    + expected);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Env helpers
    // -------------------------------------------------------------------------

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) {
            throw new IllegalArgumentException(
                "Missing required environment variable: " + name);
        }
        return v;
    }

    private static boolean optBool(String name, boolean def) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) return def;
        switch (v.trim().toLowerCase()) {
            case "true":  case "1": case "yes": case "y": case "on":  return true;
            case "false": case "0": case "no":  case "n": case "off": return false;
            default:
                throw new IllegalArgumentException(
                    "Env var '" + name + "' must be a boolean, got: " + v);
        }
    }

    private static int optInt(String name, int def) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                "Env var '" + name + "' must be an integer, got: " + v);
        }
    }
}
