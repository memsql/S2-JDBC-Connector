// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.client.result;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.util.ClosableLock;
import java.io.IOException;
import java.sql.SQLException;

public class StreamingResult extends Result {

  private static final int MAX_FETCH_SIZE = 16384;
  private final ClosableLock lock;
  private int dataFetchTime;
  private int requestedFetchSize;

  /**
   * Constructor
   *
   * @param stmt statement that initiate this result
   * @param binaryProtocol is result-set binary encoded
   * @param maxRows maximum row number
   * @param metadataList column metadata
   * @param reader packet reader
   * @param context connection context
   * @param fetchSize fetch size
   * @param lock thread safe locker
   * @param resultSetType result-set type
   * @param closeOnCompletion close statement on completion
   * @param traceEnable can network log be logged
   * @throws SQLException if any error occurs
   */
  @SuppressWarnings({"this-escape"})
  public StreamingResult(
      Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      ColumnDecoder[] metadataList,
      com.singlestore.jdbc.client.socket.Reader reader,
      Context context,
      int fetchSize,
      ClosableLock lock,
      int resultSetType,
      boolean closeOnCompletion,
      boolean traceEnable)
      throws SQLException {

    super(
        stmt,
        binaryProtocol,
        maxRows,
        metadataList,
        reader,
        context,
        resultSetType,
        closeOnCompletion,
        traceEnable,
        false,
        fetchSize);
    this.lock = lock;
    this.dataFetchTime = 0;
    this.requestedFetchSize = fetchSize;
    this.data = new byte[Math.min(MAX_FETCH_SIZE, Math.max(fetchSize, 10))][];

    addStreamingValue();
  }

  @Override
  public boolean streaming() {
    return true;
  }

  /**
   * This permit to replace current stream results by next ones.
   *
   * @throws SQLException if server return an unexpected error
   */
  private void nextStreamingValue() throws SQLException {

    // if resultSet can be back to some previous value
    if (resultSetType == TYPE_FORWARD_ONLY) {
      rowPointer = 0;
      dataSize = 0;
    }

    addStreamingValue();
  }

  @SuppressWarnings("try")
  private void addStreamingValue() throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      // read only fetchSize values
      int fetchSizeTmp =
          (maxRows <= 0)
              ? super.getFetchSize()
              : Math.min(
                  super.getFetchSize(),
                  Math.max(0, (int) (maxRows - dataFetchTime * super.getFetchSize())));
      do {
        byte[] buf = reader.readPacket(traceEnable);
        readNext(buf);
        fetchSizeTmp--;
      } while (fetchSizeTmp > 0 && !loaded);
      dataFetchTime++;
      if (maxRows > 0 && (long) dataFetchTime * super.getFetchSize() >= maxRows && !loaded)
        skipRemaining();
    } catch (IOException ioe) {
      throw exceptionFactory.create("Error while streaming resultSet data", "08000", ioe);
    }
  }

  /**
   * When protocol has a current Streaming result (this) fetch all to permit another query is
   * executing.
   *
   * @throws SQLException if any error occur
   */
  public void fetchRemaining() throws SQLException {
    if (!loaded) {
      while (!loaded) {
        addStreamingValue();
      }
      dataFetchTime++;
    }
  }

  @Override
  @SuppressWarnings("try")
  public boolean next() throws SQLException {
    checkClose();
    if (rowPointer < dataSize - 1) {
      rowPointer++;
      setRow(data[rowPointer]);
      return true;
    } else {
      if (!loaded) {
        try (ClosableLock ignore = lock.closeableLock()) {
          if (!loaded) {
            nextStreamingValue();
          }
        }

        if (resultSetType == TYPE_FORWARD_ONLY) {
          // resultSet has been cleared. next value is pointer 0.
          rowPointer = 0;
          if (dataSize > 0) {
            setRow(data[rowPointer]);
            return true;
          }
        } else {
          // cursor can move backward, so driver must keep the results.
          // results have been added to current resultSet
          rowPointer++;
          if (dataSize > rowPointer) {
            setRow(data[rowPointer]);
            return true;
          }
        }
        setNullRowBuf();
        return false;
      }

      // all data are reads and pointer is after last
      rowPointer = dataSize;
      setNullRowBuf();
      return false;
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClose();
    if (rowPointer < dataSize) {
      // has remaining results
      return false;
    } else {
      // has read all data and pointer is after last result
      // so result would have to always to be true,
      // but when result contain no row at all jdbc say that must return false
      return dataSize > 0 || dataFetchTime > 1;
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClose();
    if (resultSetType == TYPE_FORWARD_ONLY) {
      return rowPointer == 0 && dataSize > 0 && dataFetchTime == 1;
    } else {
      return rowPointer == 0 && dataSize > 0;
    }
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClose();
    if (rowPointer < dataSize - 1) {
      return false;
    } else if (loaded) {
      return rowPointer == dataSize - 1 && dataSize > 0;
    } else {
      // when streaming and not having read all results,
      // must read next packet to know if next packet is an EOF packet or some additional data
      addStreamingValue();

      if (loaded) {
        // now driver is sure when data ends.
        return rowPointer == dataSize - 1;
      }

      // There is data remaining
      return false;
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    setNullRowBuf();
    rowPointer = -1;
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    fetchRemaining();
    setNullRowBuf();
    rowPointer = dataSize;
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();
    checkNotForwardOnly();

    rowPointer = 0;
    if (dataSize > 0) {
      setRow(data[rowPointer]);
      return true;
    }
    setNullRowBuf();
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    fetchRemaining();
    rowPointer = dataSize - 1;
    if (dataSize > 0) {
      setRow(data[rowPointer]);
      return true;
    }
    setNullRowBuf();
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    checkClose();
    if (resultSetType == TYPE_FORWARD_ONLY) {
      return 0;
    }
    return rowPointer + 1;
  }

  @Override
  public boolean absolute(int idx) throws SQLException {
    checkClose();
    checkNotForwardOnly();

    if (idx == 0) {
      rowPointer = -1;
      setNullRowBuf();
      return false;
    }

    if (idx > 0 && idx <= dataSize) {
      rowPointer = idx - 1;
      setRow(data[rowPointer]);
      return true;
    }

    // if streaming, must read additional results.
    fetchRemaining();

    if (idx > 0) {
      if (idx <= dataSize) {
        rowPointer = idx - 1;
        setRow(data[rowPointer]);
        return true;
      }

      rowPointer = dataSize; // go to afterLast() position
      setNullRowBuf();

    } else {

      if (dataSize + idx >= 0) {
        // absolute position reverse from ending resultSet
        rowPointer = dataSize + idx;
        setRow(data[rowPointer]);
        return true;
      }
      setNullRowBuf();
      rowPointer = -1; // go to before first position
    }
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClose();
    int newPos = rowPointer + rows;
    if (newPos <= -1) {
      checkNotForwardOnly();
      rowPointer = -1;
      setNullRowBuf();
      return false;
    }

    while (newPos >= dataSize) {
      if (loaded) {
        rowPointer = dataSize;
        setNullRowBuf();
        return false;
      }
      addStreamingValue();
    }

    rowPointer = newPos;
    setRow(data[rowPointer]);
    return true;
  }

  @Override
  public boolean previous() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    if (rowPointer > -1) {
      rowPointer--;
      if (rowPointer != -1) {
        setRow(data[rowPointer]);
        return true;
      }
    }
    setNullRowBuf();
    return false;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClose();
    return requestedFetchSize;
  }

  @Override
  public void setFetchSize(int fetchSize) throws SQLException {
    // ensure huge fetch size won't create OOM because of array size exceeding VM limit
    // when using fetchSize with value different from 0, value must be small because goal is to
    // ensure not having too
    // much data in memory
    // so fetch size when explicitly different from 0 is limited to 16K rows
    super.setFetchSize(Math.min(MAX_FETCH_SIZE, fetchSize));
    this.requestedFetchSize = fetchSize;
    checkClose();
    if (fetchSize == 0) {
      // fetch all results
      while (!this.loaded) {
        addStreamingValue();
      }
    }
  }
}
