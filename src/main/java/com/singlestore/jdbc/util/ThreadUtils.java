// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.util;

import java.util.concurrent.Callable;
import javax.security.auth.Subject;

public class ThreadUtils {
  public static long getId(Thread thread) {
    // must be return thread.threadId() for java 19+,
    // but since we support java 8, cannot be removed for now
    return thread.getId();
  }

  public static void callAs(
      final Subject subject, final Callable<java.security.PrivilegedExceptionAction<Void>> action)
      throws Exception {
    Subject.doAs(subject, action.call());
    // must be for java 18+, but since we support java 8, cannot be removed for now
    // Subject.callAs(subject, action);
  }
}
