// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc;

import com.singlestore.jdbc.export.HaMode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HostAddress {

  public final String host;
  public int port;
  private Long threadsConnected;
  private Long threadConnectedTimeout;

  /**
   * Constructor.
   *
   * @param host host
   * @param port port
   */
  private HostAddress(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public static HostAddress from(String host, int port) {
    return new HostAddress(host, port);
  }

  /**
   * parse - parse server addresses from the URL fragment.
   *
   * @param spec list of endpoints in one of the forms 1 - host1,....,hostN:port (missing port
   *     default to SingleStore default 3306 2 - host:port,...,host:port
   * @param haMode High availability mode
   * @throws SQLException for wrong spec
   * @return parsed endpoints
   */
  public static List<HostAddress> parse(String spec, HaMode haMode) throws SQLException {
    if ("".equals(spec)) {
      return new ArrayList<>(0);
    }
    String[] tokens = spec.trim().split(",");
    int size = tokens.length;
    List<HostAddress> arr = new ArrayList<>(size);

    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      if (token.startsWith("address=")) {
        arr.add(parseParameterHostAddress(token));
      } else {
        arr.add(parseSimpleHostAddress(token));
      }
    }

    return arr;
  }

  private static HostAddress parseSimpleHostAddress(String str) throws SQLException {
    String host;
    int port = 3306;

    if (str.charAt(0) == '[') {
      /* IPv6 addresses in URLs are enclosed in square brackets */
      int ind = str.indexOf(']');
      host = str.substring(1, ind);
      if (ind != (str.length() - 1) && str.charAt(ind + 1) == ':') {
        port = getPort(str.substring(ind + 2));
      }
    } else if (str.contains(":")) {
      /* Parse host:port */
      String[] hostPort = str.split(":");
      host = hostPort[0];
      port = getPort(hostPort[1]);
    } else {
      /* Just host name is given */
      host = str;
    }
    return new HostAddress(host, port);
  }

  private static int getPort(String portString) throws SQLException {
    try {
      return Integer.parseInt(portString);
    } catch (NumberFormatException nfe) {
      throw new SQLException("Incorrect port value : " + portString);
    }
  }

  private static HostAddress parseParameterHostAddress(String str) throws SQLException {
    String host = null;
    int port = 3306;

    String[] array = str.replace(" ", "").split("(?=\\()|(?<=\\))");
    for (int i = 1; i < array.length; i++) {
      String[] token = array[i].replace("(", "").replace(")", "").trim().split("=");
      if (token.length != 2) {
        throw new IllegalArgumentException(
            "Invalid connection URL, expected key=value pairs, found " + array[i]);
      }
      String key = token[0].toLowerCase();
      String value = token[1].toLowerCase();

      switch (key) {
        case "host":
          host = value.replace("[", "").replace("]", "");
          break;
        case "port":
          port = getPort(value);
          break;
      }
    }
    return new HostAddress(host, port);
  }

  @Override
  public String toString() {
    return String.format("address=(host=%s)(port=%s)", host, port);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostAddress that = (HostAddress) o;
    return port == that.port && Objects.equals(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  public Long getThreadsConnected() {
    return threadsConnected;
  }

  public void setThreadsConnected(long threadsConnected) {
    this.threadsConnected = threadsConnected;
    // timeout in 3 minutes
    this.threadConnectedTimeout = System.currentTimeMillis() + 3 * 60 * 1000;
  }

  public void forceThreadsConnected(long threadsConnected, long threadConnectedTimeout) {
    this.threadsConnected = threadsConnected;
    this.threadConnectedTimeout = threadConnectedTimeout;
  }

  public Long getThreadConnectedTimeout() {
    return threadConnectedTimeout;
  }
}
