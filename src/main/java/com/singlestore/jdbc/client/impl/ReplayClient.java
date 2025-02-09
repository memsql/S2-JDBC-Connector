// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.impl;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.context.RedoContext;
import com.singlestore.jdbc.client.util.ClosableLock;
import com.singlestore.jdbc.export.MaxAllowedPacketException;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.message.client.PreparePacket;
import com.singlestore.jdbc.message.client.RedoableClientMessage;
import com.singlestore.jdbc.message.client.RedoableWithPrepareClientMessage;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/** Replay client wrapper */
public class ReplayClient extends StandardClient {

  private final Logger logger;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param hostAddress host
   * @param lock thread lock object
   * @param skipPostCommands must skip connection post commands
   * @throws SQLException if connection fails
   */
  public ReplayClient(
      Configuration conf, HostAddress hostAddress, ClosableLock lock, boolean skipPostCommands)
      throws SQLException {
    super(conf, hostAddress, lock, skipPostCommands);
    logger = Loggers.getLogger(ReplayClient.class);
  }

  @Override
  public int sendQuery(ClientMessage message) throws SQLException {
    checkNotClosed();
    try {
      if (message instanceof RedoableClientMessage)
        ((RedoableClientMessage) message).ensureReplayable(context);
      return message.encode(writer, context);
    } catch (IOException ioException) {
      if (ioException instanceof MaxAllowedPacketException) {
        if (((MaxAllowedPacketException) ioException).isMustReconnect()) {
          destroySocket();
          throw exceptionFactory
              .withSql(message.description())
              .create(
                  "Packet too big for current server max_allowed_packet value",
                  "08000",
                  ioException);
        }
        throw exceptionFactory
            .withSql(message.description())
            .create(
                "Packet too big for current server max_allowed_packet value", "HZ000", ioException);
      }
      destroySocket();
      throw exceptionFactory
          .withSql(message.description())
          .create("Socket error", "08000", ioException);
    }
  }

  @Override
  public List<Completion> executePipeline(
      ClientMessage[] messages,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    List<Completion> res =
        super.executePipeline(
            messages,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion,
            canRedo);
    ((RedoContext) context).saveRedo(messages);
    return res;
  }

  @Override
  public List<Completion> execute(
      ClientMessage message,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    List<Completion> completions =
        super.execute(
            message,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion,
            canRedo);
    ((RedoContext) context).saveRedo(message);
    return completions;
  }

  /**
   * Replay transaction, re-prepare server command if needed
   *
   * @param transactionSaver transaction cache
   * @throws SQLException if any error occurs
   */
  public void transactionReplay(TransactionSaver transactionSaver) throws SQLException {
    RedoableClientMessage[] buffers = transactionSaver.getBuffers();
    try {
      // replay all but last
      Prepare prepare;
      for (int i = 0; i < transactionSaver.getIdx(); i++) {
        RedoableClientMessage querySaver = buffers[i];
        int responseNo;
        if (querySaver instanceof RedoableWithPrepareClientMessage) {
          // command is a prepare statement query
          // redo on new connection need to re-prepare query
          // and substitute statement id
          RedoableWithPrepareClientMessage redoable =
              ((RedoableWithPrepareClientMessage) querySaver);
          String cmd = redoable.getCommand();
          prepare = context.getPrepareCacheCmd(cmd, redoable.prep());
          if (prepare == null) {
            PreparePacket preparePacket = new PreparePacket(cmd);
            sendQuery(preparePacket);
            prepare = (PrepareResultPacket) readPacket(preparePacket);
            logger.info("replayed command after failover: " + preparePacket.description());
          }
          responseNo = querySaver.reEncode(writer, context, prepare);
        } else {
          responseNo = querySaver.reEncode(writer, context, null);
        }
        logger.info("replayed command after failover: " + querySaver.description());
        for (int j = 0; j < responseNo; j++) {
          readResponse(querySaver);
        }
      }
    } catch (IOException e) {
      throw context
          .getExceptionFactory()
          .create("Socket error during transaction replay", "08000", e);
    }
  }
}
