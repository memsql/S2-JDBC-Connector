// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.util.constants;

/** The list of Server status flags supported in SingleStore. */
public class ServerStatus {

  /** is in transaction */
  public static final short IN_TRANSACTION = 1;
  /** autocommit */
  public static final short AUTOCOMMIT = 2;
  /** more result exists (packet follows) */
  public static final short MORE_RESULTS_EXISTS = 8;
}
