// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.
package com.singlestore.jdbc;

import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.impl.FailoverClient;
import com.singlestore.jdbc.client.impl.ReplayClient;
import com.singlestore.jdbc.client.impl.StandardClient;
import com.singlestore.jdbc.client.util.ClosableLock;
import com.singlestore.jdbc.pool.Pools;
import com.singlestore.jdbc.util.VersionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Driver implements java.sql.Driver {

  private static final Pattern identifierPattern =
      Pattern.compile("[0-9a-zA-Z$_\\u0080-\\uFFFF]*", Pattern.UNICODE_CASE);

  private static final Pattern escapePattern = Pattern.compile("[\u0000'\"\b\n\r\t\u001A\\\\]");

  private static final Map<String, String> mapper = new HashMap<>();

  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      // eat
    }
    mapper.put("\u0000", "\\0");
    mapper.put("'", "\\\\'");
    mapper.put("\"", "\\\\\"");
    mapper.put("\b", "\\\\b");
    mapper.put("\n", "\\\\n");
    mapper.put("\r", "\\\\r");
    mapper.put("\t", "\\\\t");
    mapper.put("\u001A", "\\\\Z");
    mapper.put("\\", "\\\\");
  }

  /**
   * Connect according to configuration
   *
   * @param configuration configuration
   * @return a Connection
   * @throws SQLException if connect fails
   */
  public static Connection connect(Configuration configuration) throws SQLException {
    ClosableLock lock = new ClosableLock();
    Client client;
    switch (configuration.haMode()) {
      case LOADBALANCE:
      case SEQUENTIAL:
        client = new FailoverClient(configuration, lock);
        break;

      default:
        ClientInstance<Configuration, HostAddress, ClosableLock, Boolean, Client> clientInstance =
            (configuration.transactionReplay()) ? ReplayClient::new : StandardClient::new;

        if (configuration.addresses().isEmpty()) {
          // unix socket / windows pipe
          client = clientInstance.apply(configuration, null, lock, false);
        } else {
          // loop until finding
          SQLException lastException = null;
          for (HostAddress host : configuration.addresses()) {
            try {
              client = clientInstance.apply(configuration, host, lock, false);
              return new Connection(configuration, lock, client);
            } catch (SQLException e) {
              lastException = e;
            }
          }
          throw lastException;
        }
        break;
    }
    return new Connection(configuration, lock, client);
  }

  @FunctionalInterface
  private interface ClientInstance<T, U, V, W, R> {
    R apply(T t, U u, V v, W w) throws SQLException;
  }

  /**
   * Connect to the given connection string.
   *
   * @param url the url to connect to
   * @return a connection
   * @throws SQLException if it is not possible to connect
   */
  public Connection connect(final String url, final Properties props) throws SQLException {
    Configuration configuration = Configuration.parse(url, props);
    if (configuration != null) {
      if (configuration.pool()) {
        return Pools.retrievePool(configuration).getPoolConnection().getConnection();
      }
      return connect(configuration);
    }
    return null;
  }

  /**
   * returns true if the driver can accept the url.
   *
   * @param url the url to test
   * @return true if the url is valid for this driver
   */
  @Override
  public boolean acceptsURL(String url) {
    return Configuration.acceptsUrl(url);
  }

  /**
   * Get the property info.
   *
   * @param url the url to get properties for
   * @param info the info props
   * @return all possible connector options
   * @throws SQLException if there is a problem getting the property info
   */
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Configuration conf = Configuration.parse(url, info);
    if (conf == null) {
      return new DriverPropertyInfo[0];
    }

    Properties propDesc = new Properties();
    try (InputStream inputStream =
        Driver.class.getClassLoader().getResourceAsStream("driver.properties")) {
      propDesc.load(inputStream);
    } catch (IOException io) {
      // eat
    }

    List<DriverPropertyInfo> props = new ArrayList<>();
    for (Field field : Configuration.Builder.class.getDeclaredFields()) {
      if (!field.getName().startsWith("_")) {
        try {
          Field fieldConf = Configuration.class.getDeclaredField(field.getName());
          fieldConf.setAccessible(true);
          Object obj = fieldConf.get(conf);
          String value = obj == null ? null : obj.toString();
          DriverPropertyInfo propertyInfo = new DriverPropertyInfo(field.getName(), value);
          propertyInfo.description = value == null ? "" : (String) propDesc.get(field.getName());
          propertyInfo.required = false;
          props.add(propertyInfo);
        } catch (IllegalAccessException | NoSuchFieldException e) {
          // eat error
        }
      }
    }
    return props.toArray(new DriverPropertyInfo[0]);
  }

  /**
   * gets the major version of the driver.
   *
   * @return the major versions
   */
  public int getMajorVersion() {
    return VersionFactory.getInstance().getMajorVersion();
  }

  /**
   * gets the minor version of the driver.
   *
   * @return the minor version
   */
  public int getMinorVersion() {
    return VersionFactory.getInstance().getMinorVersion();
  }

  /**
   * checks if the driver is jdbc compliant.
   *
   * @return true since the driver is not compliant
   */
  public boolean jdbcCompliant() {
    return true;
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("Use logging parameters for enabling logging.");
  }

  public static String enquoteIdentifier(String identifier, boolean alwaysQuote)
      throws SQLException {
    int len = identifier.length();

    if (isSimpleIdentifier(identifier)) {
      if (len < 1 || len > 256) {
        throw new SQLException("Invalid identifier length");
      }
      if (alwaysQuote) return "`" + identifier + "`";

      // Identifier names may begin with a numeral, but can't only contain numerals unless quoted.
      for (int i = 0; i < identifier.length(); i++) {
        if (!Character.isDigit(identifier.charAt(i))) {
          return identifier;
        }
      }
      // identifier containing only numerals must be quoted
      return "`" + identifier + "`";
    } else {
      if (identifier.contains("\u0000")) {
        throw new SQLException("Invalid name - containing u0000 character", "42000");
      }

      if (identifier.matches("^`.+`$")) {
        identifier = identifier.substring(1, identifier.length() - 1);
      }
      if (len < 1 || len > 256) {
        throw new SQLException("Invalid identifier length");
      }
      return "`" + identifier.replace("`", "``") + "`";
    }
  }

  /**
   * Enquote String value.
   *
   * @param val string value to enquote
   * @return enquoted string value
   */
  // @Override when not supporting java 8
  public static String enquoteLiteral(String val) {
    Matcher matcher = escapePattern.matcher(val);
    StringBuffer escapedVal = new StringBuffer("'");

    while (matcher.find()) {
      matcher.appendReplacement(escapedVal, mapper.get(matcher.group()));
    }
    matcher.appendTail(escapedVal);
    escapedVal.append("'");
    return escapedVal.toString();
  }

  /**
   * Retrieves whether identifier is a simple SQL identifier. The first character is an alphabetic
   * character from a through z, or from A through Z The string only contains alphanumeric
   * characters or the characters "_" and "$"
   *
   * @param identifier identifier
   * @return true if identifier doesn't have to be quoted
   * @see <a href="https://mariadb.com/kb/en/library/identifier-names/">mariadb identifier name</a>
   */
  public static boolean isSimpleIdentifier(String identifier) {
    return identifier != null
        && !identifier.isEmpty()
        && identifierPattern.matcher(identifier).matches();
  }
}
