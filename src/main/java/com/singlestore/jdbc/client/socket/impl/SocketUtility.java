// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.socket.impl;

import com.singlestore.jdbc.client.impl.ConnectionHelper;
import com.sun.jna.Platform;
import java.io.IOException;

/**
 * Socket Utility, to defined function that will create socket according to dependency and
 * configuration
 */
public class SocketUtility {

  /**
   * Create socket according to options. In case of compilation ahead of time, will throw an error
   * if dependencies found, then use default socket implementation.
   *
   * @return Socket
   */
  @SuppressWarnings("unchecked")
  public static SocketHandlerFunction getSocketHandler() {
    // forcing use of JNA to ensure AOT compilation
    Platform.getOSType();

    return (conf, hostAddress) -> {
      if (conf.pipe() != null) {
        return new NamedPipeSocket(hostAddress != null ? hostAddress.host : null, conf.pipe());
      } else if (conf.localSocket() != null) {
        try {
          return new UnixDomainSocket(conf.localSocket());
        } catch (RuntimeException re) {
          throw new IOException(re.getMessage(), re.getCause());
        }
      } else {
        return ConnectionHelper.standardSocket(conf, hostAddress);
      }
    };
  }
}
