// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.message;

import com.singlestore.jdbc.BasePreparedStatement;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.impl.StandardReadableByteBuf;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.client.result.StreamingResult;
import com.singlestore.jdbc.client.result.UpdatableResult;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.ClosableLock;
import com.singlestore.jdbc.client.util.Parameters;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.message.server.ErrorPacket;
import com.singlestore.jdbc.message.server.OkPacket;
import com.singlestore.jdbc.util.constants.ServerStatus;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

public interface ClientMessage {

  /**
   * Check that file requested correspond to request.
   *
   * @param sql current command sql
   * @param parameters current command parameter
   * @param fileName file path request
   * @param context current connection context
   * @return true if file name correspond to demand and query is a load local infile
   */
  static boolean validateLocalFileName(
      String sql, Parameters parameters, String fileName, Context context) {
    String reg =
        "^((\\s[--]|#).*(\\r"
            + "\\n"
            + "|\\r"
            + "|\\n"
            + ")|\\s*/\\*([^*]|\\*[^/])*\\*/|.)*\\s*LOAD\\s+(DATA\\s+)?LOCAL\\s+INFILE\\s+'"
            + Pattern.quote(fileName.replace("\\", "\\\\"))
            + "'";

    Pattern pattern = Pattern.compile(reg, Pattern.CASE_INSENSITIVE);
    if (pattern.matcher(sql).find()) {
      return true;
    }

    if (parameters != null) {
      pattern =
          Pattern.compile(
              "^((\\s[--]|#).*(\\r"
                  + "\\n"
                  + "|\\r"
                  + "|\\n"
                  + ")|\\s*/\\*([^*]|\\*[^/])*\\*/|.)*\\s*LOAD\\s+(DATA\\s+)?LOCAL\\s+INFILE\\s+\\?",
              Pattern.CASE_INSENSITIVE);
      if (pattern.matcher(sql).find() && parameters.size() > 0) {
        String paramString = parameters.get(0).bestEffortStringValue(context);
        if (paramString != null) {
          return paramString.equalsIgnoreCase("'" + fileName.replace("\\", "\\\\") + "'");
        }
        return true;
      }
    }
    return false;
  }
  /**
   * Encode client message to socket.
   *
   * @param writer socket writer
   * @param context connection context
   * @return number of client message written
   * @throws IOException if socket error occur
   * @throws SQLException if any issue occurs
   */
  int encode(Writer writer, Context context) throws IOException, SQLException;

  /**
   * Number of parameter rows, and so expected return length
   *
   * @return batch update length
   */
  default int batchUpdateLength() {
    return 0;
  }

  /**
   * Message description
   *
   * @return description
   */
  default String description() {
    return null;
  }

  /**
   * Are return value encoded in binary protocol
   *
   * @return use binary protocol
   */
  default boolean binaryProtocol() {
    return false;
  }

  /**
   * Can skip metadata
   *
   * @return can skip metadata
   */
  default boolean canSkipMeta() {
    return false;
  }

  /**
   * default packet resultset parser
   *
   * @param stmt caller
   * @param fetchSize fetch size
   * @param maxRows maximum number of rows
   * @param resultSetConcurrency resultset concurrency
   * @param resultSetType resultset type
   * @param closeOnCompletion must close caller on result parsing end
   * @param reader packet reader
   * @param writer packet writer
   * @param context connection context
   * @param exceptionFactory connection exception factory
   * @param lock thread safe locks
   * @param traceEnable is logging trace enable
   * @param message client message
   * @return results
   * @throws IOException if any socket error occurs
   * @throws SQLException for other kind of errors
   */
  default Completion readPacket(
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      com.singlestore.jdbc.client.socket.Reader reader,
      Writer writer,
      Context context,
      ExceptionFactory exceptionFactory,
      ClosableLock lock,
      boolean traceEnable,
      ClientMessage message)
      throws IOException, SQLException {

    ReadableByteBuf buf = reader.readReusablePacket(traceEnable);

    switch (buf.getByte()) {

        // *********************************************************************************************************
        // * OK response
        // *********************************************************************************************************
      case (byte) 0x00:
        return new OkPacket(buf, context);

        // *********************************************************************************************************
        // * ERROR response
        // *********************************************************************************************************
      case (byte) 0xff:
        // force current status to in transaction to ensure rollback/commit, since command may
        // have issue a transaction
        ErrorPacket errorPacket = new ErrorPacket(buf, context);
        throw exceptionFactory
            .withSql(this.description())
            .create(
                errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
      case (byte) 0xfb:
        buf.skip(1); // skip header
        SQLException exception = null;
        reader.getSequence().set(writer.getSequence());
        InputStream is = getLocalInfileInputStream();
        if (is == null) {
          String fileName = buf.readStringNullEnd();
          if (!message.validateLocalFileName(fileName, context)) {
            exception =
                exceptionFactory
                    .withSql(this.description())
                    .create(
                        String.format(
                            "LOAD DATA LOCAL INFILE asked for file '%s' that doesn't correspond to initial query %s. Possible malicious proxy changing server answer ! Command interrupted",
                            fileName, this.description()),
                        "HY000");
          } else {

            try {
              is = new FileInputStream(fileName);
            } catch (FileNotFoundException f) {
              exception =
                  exceptionFactory
                      .withSql(this.description())
                      .create("Could not send file : " + f.getMessage(), "HY000", f);
            }
          }
        }

        // sending stream
        if (is != null) {
          try {
            byte[] fileBuf = new byte[8192];
            int len;
            while ((len = is.read(fileBuf)) > 0) {
              writer.writeBytes(fileBuf, 0, len);
              writer.flush();
            }
          } finally {
            is.close();
          }
        }

        // after file send / having an error, sending an empty packet to keep connection state ok
        writer.writeEmptyPacket();
        Completion completion =
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
                traceEnable,
                message);
        if (exception != null) {
          throw exception;
        }
        return completion;

        // *********************************************************************************************************
        // * ResultSet
        // *********************************************************************************************************
      default:
        int fieldCount = buf.readIntLengthEncodedNotNull();

        ColumnDecoder[] ci;
        if (context.canSkipMeta() && this.canSkipMeta()) {
          if (buf.readByte() == 0) {
            // skip meta
            ci = ((BasePreparedStatement) stmt).getMeta();
          } else {
            // can skip meta, but meta might have changed
            ci = new ColumnDecoder[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
              ci[i] =
                  context
                      .getColumnDecoderFunction()
                      .apply(new StandardReadableByteBuf(reader.readPacket(traceEnable)));
            }
            ((BasePreparedStatement) stmt).updateMeta(ci);
          }
        } else {
          // always read meta
          ci = new ColumnDecoder[fieldCount];
          for (int i = 0; i < fieldCount; i++) {
            ci[i] =
                context
                    .getColumnDecoderFunction()
                    .apply(new StandardReadableByteBuf(reader.readPacket(traceEnable)));
          }
        }

        // intermediate EOF
        if (!context.isEofDeprecated()) {
          reader.skipPacket();
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

  /**
   * Get current local infile input stream.
   *
   * @return default to null
   */
  default InputStream getLocalInfileInputStream() {
    return null;
  }

  /**
   * Request for local file to be validated from current query.
   *
   * @param fileName server file request path
   * @param context current connection context
   * @return true if file name correspond to demand and query is a load local infile
   */
  default boolean validateLocalFileName(String fileName, Context context) {
    return false;
  }
}
