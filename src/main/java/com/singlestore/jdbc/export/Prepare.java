// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2023 SingleStore, Inc.

package com.singlestore.jdbc.export;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Column;
import java.sql.SQLException;

/** Prepare packet COM_STMT_PREPARE (see https://mariadb.com/kb/en/com_stmt_prepare/) */
public interface Prepare {

  /**
   * Close Prepared command
   *
   * @param con current connection
   * @throws SQLException if prepare close fails
   */
  void close(Client con) throws SQLException;

  /**
   * Decrement use of prepare. In case not used anymore, and not in cache, will be close.
   *
   * @param con connection
   * @param preparedStatement current prepared statement that was using prepare object
   * @throws SQLException if close fails
   */
  void decrementUse(Client con, ServerPreparedStatement preparedStatement) throws SQLException;

  /**
   * Get current prepare statement id
   *
   * @return statement id
   */
  int getStatementId();

  Column[] getParameters();

  Column[] getColumns();

  void setColumns(Column[] columns);
}
