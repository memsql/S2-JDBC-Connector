// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.client.util;

import java.util.concurrent.locks.ReentrantLock;

/** Utility class to permit use AutoClosable, ensuring proper lock release. */
public final class ClosableLock extends ReentrantLock implements AutoCloseable {
  private static final long serialVersionUID = -8041187539350329669L;

  /**
   * Default constructor, retaining lock.
   *
   * @return Closable lock
   */
  public ClosableLock closeableLock() {
    this.lock();
    return this;
  }

  @Override
  public void close() {
    this.unlock();
  }
}
