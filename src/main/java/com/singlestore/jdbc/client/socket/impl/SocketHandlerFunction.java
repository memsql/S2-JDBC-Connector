// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.socket.impl;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;

/** Construct socket depending on configuration helper */
@FunctionalInterface
public interface SocketHandlerFunction {

  /**
   * Create socket
   *
   * @param conf configuration
   * @param hostAddress host
   * @return socket
   * @throws IOException if any socket issue occurs
   * @throws SQLException for other kind of error
   */
  Socket apply(Configuration conf, HostAddress hostAddress) throws IOException, SQLException;
}
