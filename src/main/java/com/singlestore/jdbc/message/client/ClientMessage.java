// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.BasePreparedStatement;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.*;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.client.result.StreamingResult;
import com.singlestore.jdbc.client.result.UpdatableResult;
import com.singlestore.jdbc.client.socket.PacketReader;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import com.singlestore.jdbc.message.server.Completion;
import com.singlestore.jdbc.message.server.ErrorPacket;
import com.singlestore.jdbc.message.server.OkPacket;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.exceptions.ExceptionFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public interface ClientMessage {

  int encode(PacketWriter writer, Context context) throws IOException, SQLException;

  default int batchUpdateLength() {
    return 0;
  }

  default String description() {
    return null;
  }

  default boolean binaryProtocol() {
    return false;
  }

  default boolean canSkipMeta() {
    return false;
  }

  static final Logger logger =
      Logger.getLogger("com.singlestore.jdbc.message.client.ClientMessage");

  default Completion readPacket(
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      PacketReader reader,
      PacketWriter writer,
      Context context,
      ExceptionFactory exceptionFactory,
      ReentrantLock lock,
      boolean traceEnable)
      throws IOException, SQLException {

    ReadableByteBuf buf = reader.readPacket(true, traceEnable);

    switch (buf.getUnsignedByte()) {

        // *********************************************************************************************************
        // * OK response
        // *********************************************************************************************************
      case 0x00:
        return new OkPacket(buf, context);

        // *********************************************************************************************************
        // * ERROR response
        // *********************************************************************************************************
      case 0xff:
        // force current status to in transaction to ensure rollback/commit, since command may
        // have issue a transaction
        ErrorPacket errorPacket = new ErrorPacket(buf, context);
        throw exceptionFactory
            .withSql(this.description())
            .create(
                errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

        // *********************************************************************************************************
        // * LOCAL_INFILE response
        // *********************************************************************************************************
      case 0xfb:
        buf.skip(1); // skip header
        String fileName = buf.readStringNullEnd();
        InputStream is = stmt.getNextLocalInfileInputStream();
        stmt.setNextLocalInfileInputStream(null);

        if (is == null) {
          try {
            is = new FileInputStream(fileName);
          } catch (FileNotFoundException f) {
            writer.writeEmptyPacket();
            readPacket(
                stmt,
                fetchSize,
                maxRows,
                resultSetConcurrency,
                resultSetType,
                closeOnCompletion,
                reader,
                writer,
                context,
                exceptionFactory,
                lock,
                traceEnable);
            throw exceptionFactory
                .withSql(this.description())
                .create("Could not send file : " + f.getMessage(), "HY000", f);
          }
        }

        try {
          byte[] streamBuf = new byte[8192];
          int len;
          while (true) {
            try {
              len = is.read(streamBuf);
            } catch (IOException e) {
              throw exceptionFactory
                  .withSql(this.description())
                  .create("Could not read the input stream : " + e.getMessage(), "HY000", e);
            }
            if (len <= 0) {
              break;
            }
            writer.writeBytes(streamBuf, 0, len);
            writer.flush();
          }
        } finally {
          is.close();
        }
        writer.writeEmptyPacket();
        return readPacket(
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion,
            reader,
            writer,
            context,
            exceptionFactory,
            lock,
            traceEnable);

        // *********************************************************************************************************
        // * ResultSet
        // *********************************************************************************************************
      default:
        int fieldCount = buf.readLengthNotNull();

        ColumnDefinitionPacket[] ci;
        if (context.canSkipMeta() && this.canSkipMeta()) {
          boolean skipMeta = buf.readByte() == 0;
          if (skipMeta) {
            ci = ((BasePreparedStatement) stmt).getMeta();
          } else {
            // read columns information's
            ci = new ColumnDefinitionPacket[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
              ci[i] =
                  new ColumnDefinitionPacket(
                      reader.readPacket(false, traceEnable), context.isExtendedInfo());
            }
            ((BasePreparedStatement) stmt).updateMeta(ci);
            if (!context.isEofDeprecated()) {
              // skip intermediate EOF
              reader.readPacket(true, traceEnable);
            }
          }
        } else {
          // read columns information's
          ci = new ColumnDefinitionPacket[fieldCount];
          for (int i = 0; i < fieldCount; i++) {
            ci[i] =
                new ColumnDefinitionPacket(
                    reader.readPacket(false, traceEnable), context.isExtendedInfo());
          }
          if (!context.isEofDeprecated()) {
            // skip intermediate EOF
            reader.readPacket(true, traceEnable);
          }
        }

        // read resultSet
        if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
          return new UpdatableResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        }

        logger.info("FETCH SIZE " + fetchSize);
        if (fetchSize != 0) {
          if ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
            context.setServerStatus(context.getServerStatus() - ServerStatus.MORE_RESULTS_EXISTS);
          }

          return new StreamingResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              fetchSize,
              lock,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        } else {
          return new CompleteResult(
              stmt,
              binaryProtocol(),
              maxRows,
              ci,
              reader,
              context,
              resultSetType,
              closeOnCompletion,
              traceEnable);
        }
    }
  }
}
