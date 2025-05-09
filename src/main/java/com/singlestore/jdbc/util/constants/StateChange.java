// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.util.constants;

@SuppressWarnings("unused")
public final class StateChange {
  /** system variable change */
  public static final short SESSION_TRACK_SYSTEM_VARIABLES = 0;
  /** schema change */
  public static final short SESSION_TRACK_SCHEMA = 1;
  /** state change */
  public static final short SESSION_TRACK_STATE_CHANGE = 2;
  /** GTID change */
  public static final short SESSION_TRACK_GTIDS = 3;
  /** transaction characteristics change */
  public static final short SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 4;
  /** transaction state change */
  public static final short SESSION_TRACK_TRANSACTION_STATE = 5;
}
