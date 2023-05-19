// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.socket;

import com.google.common.util.concurrent.RateLimiter;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Permit to buf socket data, reading not only asked bytes, but available number of bytes when
 * possible.
 */
public class ReadAheadBufferedStream extends FilterInputStream {
  private static final Logger logger = Loggers.getLogger(ReadAheadBufferedStream.class);

  private static final int BUF_SIZE = 16384;
  private final byte[] buf;
  private int end;
  private int pos;
  private RateLimiter rateLimiter = null;

  public ReadAheadBufferedStream(InputStream in, long readAheadInputRateLimit) {
    super(in);
    buf = new byte[BUF_SIZE];
    end = 0;
    pos = 0;
    // logger.info(String.format("SingleStore JDBC: rate limit %d", readAheadInputRateLimit));
    if (readAheadInputRateLimit != 0) {
      rateLimiter = RateLimiter.create(readAheadInputRateLimit);
    }
  }

  /**
   * Reading one byte from cache of socket if needed.
   *
   * @return byte value
   * @throws IOException if socket reading error.
   */
  public synchronized int read() throws IOException {
    if (pos >= end) {
      fillbuf(1);
      if (pos >= end) {
        return -1;
      }
    }
    return buf[pos++] & 0xff;
  }

  /**
   * Returing byte array, from cache of reading socket if needed.
   *
   * @param externalBuf buf to fill
   * @param off offset
   * @param len length to read
   * @return number of added bytes
   * @throws IOException if exception during socket reading
   */
  public synchronized int read(byte[] externalBuf, int off, int len) throws IOException {

    if (len == 0) {
      return 0;
    }

    int totalReads = 0;
    while (true) {

      // read
      if (end - pos <= 0) {
        if (len - totalReads >= buf.length) {
          // buf length is less than asked byte and buf is empty
          // => filling directly into external buf
          // logger.info(
          //    String.format(
          //        "SINGELSTORE JDBC: Reading %d bytes from socket (%h)", len - totalReads, this));
          int reads = super.read(externalBuf, off + totalReads, len - totalReads);
          if (rateLimiter != null) {
            // logger.info(String.format("SINGELSTORE JDBC: rate limiter %d", reads));
            rateLimiter.acquire(reads);
          }

          // logger.info(
          //    String.format("SINGELSTORE JDBC: Read %d bytes from socket (%h)", reads, this));
          if (reads <= 0) {
            return (totalReads == 0) ? -1 : totalReads;
          }
          return totalReads + reads;

        } else {

          // filling internal buf
          fillbuf(len - totalReads);
          if (end <= 0) {
            return (totalReads == 0) ? -1 : totalReads;
          }
        }
      }

      // copy internal value to buf.
      int copyLength = Math.min(len - totalReads, end - pos);
      System.arraycopy(buf, pos, externalBuf, off + totalReads, copyLength);
      pos += copyLength;
      totalReads += copyLength;

      if (totalReads >= len || super.available() <= 0) {
        return totalReads;
      }
    }
  }

  /**
   * Fill buf with required length, or available bytes.
   *
   * @param minNeededBytes asked number of bytes
   * @throws IOException in case of failing reading stream.
   */
  private void fillbuf(int minNeededBytes) throws IOException {
    int lengthToReallyRead = Math.min(BUF_SIZE, Math.max(super.available(), minNeededBytes));
    // logger.info(
    //    String.format(
    //        "SINGELSTORE JDBC: Reading %d bytes from socket (%h)", lengthToReallyRead, this));
    end = super.read(buf, 0, lengthToReallyRead);
    if (rateLimiter != null) {
      // logger.info(String.format("SINGELSTORE JDBC: rate limiter %d", end));
      rateLimiter.acquire(end);
    }
    // logger.info(String.format("SINGELSTORE JDBC: Read %d bytes from socket (%h)", end, this));
    pos = 0;
  }

  public synchronized long skip(long n) throws IOException {
    throw new IOException("Skip from socket not implemented");
  }

  public synchronized int available() throws IOException {
    return end - pos + super.available();
  }

  public synchronized void reset() throws IOException {
    throw new IOException("reset from socket not implemented");
  }

  public boolean markSupported() {
    return false;
  }

  public void close() throws IOException {
    super.close();
    end = 0;
    pos = 0;
  }
}
