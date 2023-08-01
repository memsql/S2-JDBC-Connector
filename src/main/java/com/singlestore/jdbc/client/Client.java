// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executor;

public interface Client extends AutoCloseable {

  List<Completion> execute(ClientMessage message) throws SQLException;

  List<Completion> execute(ClientMessage message, com.singlestore.jdbc.Statement stmt)
      throws SQLException;

  List<Completion> execute(
      ClientMessage message,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException;

  List<Completion> executePipeline(
      ClientMessage[] messages,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException;

  void readStreamingResults(
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException;

  void closePrepare(Prepare prepare) throws SQLException;

  void abort(Executor executor) throws SQLException;

  void close() throws SQLException;

  void setReadOnly(boolean readOnly) throws SQLException;

  int getWaitTimeout();

  int getSocketTimeout();

  void setSocketTimeout(int milliseconds) throws SQLException;

  boolean isClosed();

  void reset();

  boolean isPrimary();

  Context getContext();

  ExceptionFactory getExceptionFactory();

  HostAddress getHostAddress();
}
