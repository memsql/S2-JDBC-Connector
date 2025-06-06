// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.pool;

public interface PoolMBean {
  /**
   * get pool active connection number
   *
   * @return pool active connection number
   */
  long getActiveConnections();

  /**
   * get pool total connection
   *
   * @return pool total connection number
   */
  long getTotalConnections();

  /**
   * get idle connection number
   *
   * @return idle connection number
   */
  long getIdleConnections();

  /**
   * get connection waiting request number
   *
   * @return request number
   */
  long getConnectionRequests();
}
