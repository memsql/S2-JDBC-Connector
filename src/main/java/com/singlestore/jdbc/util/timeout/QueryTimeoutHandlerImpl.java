// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.util.timeout;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.client.util.ClosableLock;
import com.singlestore.jdbc.client.util.SchedulerProvider;
import com.singlestore.jdbc.util.log.Loggers;
import java.util.concurrent.*;

public class QueryTimeoutHandlerImpl implements QueryTimeoutHandler {
  private Future<?> timerTaskFuture;
  private ScheduledExecutorService timeoutScheduler;
  private Connection conn;
  private ClosableLock lock;

  public QueryTimeoutHandler create(int queryTimeout) {
    assert (timerTaskFuture == null);
    if (queryTimeout > 0) {
      if (timeoutScheduler == null) timeoutScheduler = SchedulerProvider.getTimeoutScheduler(lock);
      timerTaskFuture =
          timeoutScheduler.schedule(
              () -> {
                try {
                  conn.cancelCurrentQuery();
                } catch (Throwable e) {
                  Loggers.getLogger(QueryTimeoutHandlerImpl.class.getName())
                      .error(e.getMessage(), e);
                }
              },
              queryTimeout,
              TimeUnit.SECONDS);
    }
    return this;
  }

  public QueryTimeoutHandlerImpl(Connection conn, ClosableLock lock) {
    this.conn = conn;
    this.lock = lock;
  }

  @Override
  public void close() {
    if (timerTaskFuture != null) {
      if (!timerTaskFuture.cancel(true)) {
        // could not cancel, task either started or already finished
        // we must now wait for task to finish ensuring state modifications are done
        try {
          timerTaskFuture.get();
        } catch (InterruptedException | ExecutionException | CancellationException e) {
          // ignore error, likely due to interrupting during cancel
        }
      }
      timerTaskFuture = null;
    }
  }
}
