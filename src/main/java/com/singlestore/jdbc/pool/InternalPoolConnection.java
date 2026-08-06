// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.pool;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.SingleStorePoolConnection;
import java.util.concurrent.atomic.AtomicLong;

public class InternalPoolConnection extends SingleStorePoolConnection {
  private final AtomicLong lastUsed;
  /**
   * True once this item's slot has been removed from {@code Pool.totalConnection}. Used to make
   * error/close accounting idempotent across repeated connection events.
   */
  private boolean removedFromTotal;

  /**
   * Constructor.
   *
   * @param connection connection to retrieve connection options
   */
  public InternalPoolConnection(Connection connection) {
    super(connection);
    lastUsed = new AtomicLong(System.nanoTime());
  }

  /**
   * Indicate last time this pool connection has been used.
   *
   * @return current last used time (nano).
   */
  public AtomicLong getLastUsed() {
    return lastUsed;
  }

  /** Set last poolConnection use to now. */
  public void lastUsedToNow() {
    lastUsed.set(System.nanoTime());
  }

  public boolean isRemovedFromTotal() {
    return removedFromTotal;
  }

  public void setRemovedFromTotal(boolean removedFromTotal) {
    this.removedFromTotal = removedFromTotal;
  }

  /** Reset last used time, to ensure next retrieval will validate connection before borrowing */
  public void ensureValidation() {
    lastUsed.set(0L);
  }
}
