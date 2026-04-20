/*
 * ************************************************************************************
 *   Copyright (c) 2021-2025 SingleStore, Inc.
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

import java.security.PrivilegedExceptionAction;
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
import org.ietf.jgss.Oid;

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
 *                                       GSSCredential via JAAS and pass it to the
 *                                       driver through connection Properties
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
        LoginContext lc = new LoginContext("Krb5ConnectorContext");
        lc.login();
        Subject subject = lc.getSubject();
        System.out.println("JAAS login OK, subject principals: " + subject.getPrincipals());

        GSSCredential cred = Subject.doAs(subject,
            (PrivilegedExceptionAction<GSSCredential>) () -> {
                GSSManager mgr = GSSManager.getInstance();
                Oid krb5 = new Oid("1.2.840.113554.1.2.2");
                GSSName name = mgr.createName(user, GSSName.NT_USER_NAME);
                return mgr.createCredential(name,
                    GSSCredential.DEFAULT_LIFETIME,
                    krb5,
                    GSSCredential.INITIATE_ONLY);
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
                System.out.println("current_user(): " + rs.getString("u"));
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
