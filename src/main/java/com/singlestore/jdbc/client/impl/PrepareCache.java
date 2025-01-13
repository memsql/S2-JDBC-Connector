// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.BasePreparedStatement;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.server.CachedPrepareResultPacket;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import java.util.LinkedHashMap;
import java.util.Map;

/** LRU prepare cache */
public final class PrepareCache extends LinkedHashMap<String, CachedPrepareResultPacket>
    implements com.singlestore.jdbc.client.PrepareCache {

  private static final long serialVersionUID = -8922905563713952695L;
  /** cache maximum size */
  private final int maxSize;
  /** client */
  private final transient StandardClient con;

  /**
   * LRU prepare cache constructor
   *
   * @param size cache size
   * @param con client
   */
  public PrepareCache(int size, StandardClient con) {
    super(size, .75f, true);
    this.maxSize = size;
    this.con = con;
  }

  @Override
  public boolean removeEldestEntry(Map.Entry<String, CachedPrepareResultPacket> eldest) {
    if (this.size() > maxSize) {
      eldest.getValue().unCache(con);
      return true;
    }
    return false;
  }

  @Override
  public synchronized Prepare get(String key, BasePreparedStatement preparedStatement) {
    CachedPrepareResultPacket prepare = super.get(key);
    if (prepare != null && preparedStatement != null) {
      prepare.incrementUse(preparedStatement);
    }
    return prepare;
  }

  @Override
  public synchronized Prepare put(
      String key, Prepare result, BasePreparedStatement preparedStatement) {
    CachedPrepareResultPacket cached = super.get(key);

    // if there is already some cached data, return existing cached data
    if (cached != null) {
      cached.incrementUse(preparedStatement);
      ((CachedPrepareResultPacket) result).unCache(con);
      return cached;
    }

    if (((CachedPrepareResultPacket) result).cache()) {
      ((CachedPrepareResultPacket) result).incrementUse(preparedStatement);
      super.put(key, (CachedPrepareResultPacket) result);
    }
    return null;
  }

  public CachedPrepareResultPacket get(Object key) {
    throw new IllegalStateException("not available method");
  }

  /**
   * NOT USED
   *
   * @param key key
   * @param result results
   * @return will throw an exception
   */
  @SuppressWarnings("unused")
  public CachedPrepareResultPacket put(String key, PrepareResultPacket result) {
    throw new IllegalStateException("not available method");
  }

  public void reset() {
    for (CachedPrepareResultPacket prep : values()) {
      prep.reset();
    }
    this.clear();
  }
}
