// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import java.io.*;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

public class SingleStoreClob implements Clob, NClob, Serializable {

  private static final long serialVersionUID = -3066501059817815286L;

  protected byte[] data;
  protected transient int offset;
  protected transient int length;

  /** Creates an empty Clob. */
  public SingleStoreClob() {
    data = new byte[0];
    offset = 0;
    length = 0;
  }

  /**
   * Creates a Clob with content.
   *
   * @param bytes the content for the Clob.
   */
  public SingleStoreClob(byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("byte array is null");
    }
    data = bytes;
    offset = 0;
    length = bytes.length;
  }

  /**
   * Creates a Clob with content.
   *
   * @param bytes the content for the Clob.
   * @param offset offset
   * @param length length
   */
  public SingleStoreClob(byte[] bytes, int offset, int length) {
    if (bytes == null) {
      throw new IllegalArgumentException("byte array is null");
    }
    data = bytes;
    this.offset = offset;
    this.length = Math.min(bytes.length - offset, length);
  }

  private SingleStoreClob(int offset, int length, byte[] bytes) {
    this.data = bytes;
    this.offset = offset;
    this.length = length;
  }

  public static SingleStoreClob safeSingleStoreClob(byte[] bytes, int offset, int length) {
    return new SingleStoreClob(offset, length, bytes);
  }

  /**
   * ToString implementation.
   *
   * @return string value of clob content.
   */
  public String toString() {
    return new String(data, offset, length, StandardCharsets.UTF_8);
  }

  /**
   * Get sub string.
   *
   * @param pos position
   * @param length length of sub string
   * @return substring
   * @throws SQLException if pos is less than 1 or length is less than 0
   */
  public String getSubString(long pos, int length) throws SQLException {

    if (pos < 1) {
      throw new SQLException("position must be >= 1");
    }

    if (length < 0) {
      throw new SQLException("length must be > 0");
    }

    String val = toString();
    return val.substring((int) pos - 1, Math.min((int) pos - 1 + length, val.length()));
  }

  public Reader getCharacterStream() {
    return new StringReader(toString());
  }

  /**
   * Returns a Reader object that contains a partial Clob value, starting with the character
   * specified by pos, which is length characters in length.
   *
   * @param pos the offset to the first character of the partial value to be retrieved. The first
   *     character in the Clob is at position 1.
   * @param length the length in characters of the partial value to be retrieved.
   * @return Reader through which the partial Clob value can be read.
   * @throws SQLException if pos is less than 1 or if pos is greater than the number of characters
   *     in the Clob or if pos + length is greater than the number of characters in the Clob
   */
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    String val = toString();
    if (val.length() < (int) pos - 1 + length) {
      throw new SQLException("pos + length is greater than the number of characters in the Clob");
    }
    String sub = val.substring((int) pos - 1, (int) pos - 1 + (int) length);
    return new StringReader(sub);
  }

  /**
   * Set character stream.
   *
   * @param pos position
   * @return writer
   * @throws SQLException if position is invalid
   */
  public Writer setCharacterStream(long pos) throws SQLException {
    int bytePosition = utf8Position((int) pos - 1);
    OutputStream stream = setBinaryStream(bytePosition + 1);
    return new OutputStreamWriter(stream, StandardCharsets.UTF_8);
  }

  public InputStream getAsciiStream() throws SQLException {
    return getBinaryStream();
  }

  public long position(String searchStr, long start) {
    return toString().indexOf(searchStr, (int) start - 1) + 1;
  }

  public long position(Clob searchStr, long start) {
    return position(searchStr.toString(), start);
  }

  /**
   * Convert character position into byte position in UTF8 byte array.
   *
   * @param charPosition charPosition
   * @return byte position
   */
  private int utf8Position(int charPosition) {
    int pos = offset;
    for (int i = 0; i < charPosition; i++) {
      int byteValue = data[pos] & 0xff;
      if (byteValue < 0x80) {
        pos += 1;
      } else if (byteValue < 0xE0) {
        pos += 2;
      } else if (byteValue < 0xF0) {
        pos += 3;
      } else {
        pos += 4;
      }
    }
    return pos;
  }

  /**
   * Set String.
   *
   * @param pos position
   * @param str string
   * @return string length
   * @throws SQLException if UTF-8 conversion failed
   */
  public int setString(long pos, String str) throws SQLException {
    if (str == null) {
      throw new SQLException("cannot add null string");
    }
    if (pos < 0) {
      throw new SQLException("position must be >= 0");
    }
    int bytePosition = utf8Position((int) pos - 1);
    this.setBytes(bytePosition + 1 - offset, str.getBytes(StandardCharsets.UTF_8));
    return str.length();
  }

  public int setString(long pos, String str, int offset, int len) throws SQLException {
    if (str == null) {
      throw new SQLException("cannot add null string");
    }

    if (offset < 0) {
      throw new SQLException("offset must be >= 0");
    }

    if (len < 0) {
      throw new SQLException("len must be > 0");
    }
    return setString(pos, str.substring(offset, Math.min(offset + len, str.length())));
  }

  public OutputStream setAsciiStream(long pos) throws SQLException {
    return setBinaryStream(utf8Position((int) pos - 1) + 1);
  }

  /** Return character length of the Clob. Assume UTF8 encoding. */
  @Override
  public long length() {
    // The length of a character string is the number of UTF-16 units (not the number of characters)
    long len = 0;
    int pos = offset;

    // set ASCII (<= 127 chars)
    while (len < length && data[pos] > 0) {
      len++;
      pos++;
    }

    // multi-bytes UTF-8
    while (pos < offset + length) {
      byte firstByte = data[pos++];
      if (firstByte < 0) {
        if (firstByte >> 5 != -2 || (firstByte & 30) == 0) {
          if (firstByte >> 4 == -2) {
            if (pos + 1 < offset + length) {
              pos += 2;
              len++;
            } else {
              throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
            }
          } else if (firstByte >> 3 != -2) {
            throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
          } else if (pos + 2 < offset + length) {
            pos += 3;
            len += 2;
          } else {
            // bad truncated UTF8
            pos += offset + length;
            len += 1;
          }
        } else {
          pos++;
          len++;
        }
      } else {
        len++;
      }
    }
    return len;
  }

  @Override
  public void truncate(final long truncateLen) {

    // truncate the number of UTF-16 characters
    // this can result in a bad UTF-8 string if string finish with a
    // character represented in 2 UTF-16
    long len = 0;
    int pos = offset;

    // set ASCII (<= 127 chars)
    while (len < length && len < truncateLen && data[pos] >= 0) {
      len++;
      pos++;
    }

    // multi-bytes UTF-8
    while (pos < offset + length && len < truncateLen) {
      byte firstByte = data[pos++];
      if (firstByte < 0) {
        if (firstByte >> 5 != -2 || (firstByte & 30) == 0) {
          if (firstByte >> 4 == -2) {
            if (pos + 1 < offset + length) {
              pos += 2;
              len++;
            } else {
              throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
            }
          } else if (firstByte >> 3 != -2) {
            throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
          } else if (pos + 2 < offset + length) {
            if (len + 2 <= truncateLen) {
              pos += 3;
              len += 2;
            } else {
              // truncation will result in bad UTF-8 String
              pos += 1;
              len = truncateLen;
            }
          } else {
            throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
          }
        } else {
          pos++;
          len++;
        }
      } else {
        len++;
      }
    }
    length = pos - offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SingleStoreClob that = (SingleStoreClob) o;

    if (length != that.length) return false;

    for (int i = 0; i < length; i++) {
      if (data[offset + i] != that.data[that.offset + i]) return false;
    }
    return true;
  }

  /**
   * Retrieves all or part of the <code>CLOB</code> value that this <code>Clob</code> object
   * represents, as an array of bytes. This <code>byte</code> array contains up to <code>length
   * </code> consecutive bytes starting at position <code>pos</code>.
   *
   * @param pos the ordinal position of the first byte in the <code>Clob</code> value to be
   *     extracted; the first byte is at position 1
   * @param length the number of consecutive bytes to be copied; the value for length must be 0 or
   *     greater
   * @return a byte array containing up to <code>length</code> consecutive bytes from the <code>CLOB
   *     </code> value designated by this <code>Clob</code> object, starting with the byte at
   *     position <code>pos</code>
   * @throws SQLException if there is an error accessing the <code>Clob</code> value; if pos is less
   *     than 1 or length is less than 0
   * @see #setBytes
   * @since 1.2
   */
  public byte[] getBytes(final long pos, final int length) throws SQLException {
    if (pos < 1) {
      throw new SQLException(
          String.format("Out of range (position should be > 0, but is %s)", pos));
    }
    final int offset = this.offset + (int) (pos - 1);
    byte[] result = new byte[length];
    System.arraycopy(data, offset, result, 0, Math.min(this.length - (int) (pos - 1), length));
    return result;
  }

  /**
   * Retrieves the <code>CLOB</code> value designated by this <code>Clob</code> instance as a
   * stream.
   *
   * @return a stream containing the <code>CLOB</code> data
   * @throws SQLException if something went wrong
   * @see #setBinaryStream
   */
  public InputStream getBinaryStream() throws SQLException {
    return getBinaryStream(1, length);
  }

  /**
   * Returns an <code>InputStream</code> object that contains a partial <code>Clob</code> value,
   * starting with the byte specified by pos, which is length bytes in length.
   *
   * @param pos the offset to the first byte of the partial value to be retrieved. The first byte in
   *     the <code>Clob</code> is at position 1
   * @param length the length in bytes of the partial value to be retrieved
   * @return <code>InputStream</code> through which the partial <code>Clob</code> value can be read.
   * @throws SQLException if pos is less than 1 or if pos is greater than the number of bytes in the
   *     <code>Clob</code> or if pos + length is greater than the number of bytes in the <code>Clob
   *     </code>
   */
  public InputStream getBinaryStream(final long pos, final long length) throws SQLException {
    if (pos < 1) {
      throw new SQLException("Out of range (position should be > 0)");
    }
    if (pos - 1 > this.length) {
      throw new SQLException("Out of range (position > stream size)");
    }
    if (pos + length - 1 > this.length) {
      throw new SQLException("Out of range (position + length - 1 > streamSize)");
    }

    return new ByteArrayInputStream(data, this.offset + (int) pos - 1, (int) length);
  }

  /**
   * Writes the given array of bytes to the <code>CLOB</code> value that this <code>Clob</code>
   * object represents, starting at position <code>pos</code>, and returns the number of bytes
   * written. The array of bytes will overwrite the existing bytes in the <code>Clob</code> object
   * starting at the position <code>pos</code>. If the end of the <code>Clob</code> value is reached
   * while writing the array of bytes, then the length of the <code>Clob</code> value will be
   * increased to accommodate the extra bytes.
   *
   * @param pos the position in the <code>Clob</code> object at which to start writing; the first
   *     position is 1
   * @param bytes the array of bytes to be written to the <code>Clob</code> value that this <code>
   *     Clob</code> object represents
   * @return the number of bytes written
   * @see #getBytes
   */
  public int setBytes(final long pos, final byte[] bytes) throws SQLException {
    if (pos < 1) {
      throw new SQLException("pos should be > 0, first position is 1.");
    }

    final int arrayPos = (int) pos - 1;

    if (length > arrayPos + bytes.length) {

      System.arraycopy(bytes, 0, data, offset + arrayPos, bytes.length);

    } else {

      byte[] newContent = new byte[arrayPos + bytes.length];
      if (Math.min(arrayPos, length) > 0) {
        System.arraycopy(data, this.offset, newContent, 0, Math.min(arrayPos, length));
      }
      System.arraycopy(bytes, 0, newContent, arrayPos, bytes.length);
      data = newContent;
      length = arrayPos + bytes.length;
      offset = 0;
    }
    return bytes.length;
  }

  /**
   * Retrieves a stream that can be used to write to the <code>Clob</code> value that this <code>
   * Clob</code> object represents. The stream begins at position <code>pos</code>. The bytes
   * written to the stream will overwrite the existing bytes in the <code>Clob</code> object
   * starting at the position <code>pos</code>. If the end of the <code>Clob</code> value is reached
   * while writing to the stream, then the length of the <code>Clob</code> value will be increased
   * to accommodate the extra bytes.
   *
   * <p><b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the
   * <code>Clob</code> value then the behavior is undefined. Some JDBC drivers may throw a <code>
   * SQLException</code> while other drivers may support this operation.
   *
   * @param pos the position in the <code>Clob</code> value at which to start writing; the first
   *     position is 1
   * @return a <code>java.io.OutputStream</code> object to which data can be written
   * @throws SQLException if there is an error accessing the <code>Clob</code> value or if pos is
   *     less than 1
   * @see #getBinaryStream
   * @since 1.4
   */
  public OutputStream setBinaryStream(final long pos) throws SQLException {
    if (pos < 1) {
      throw new SQLException("Invalid position in clob");
    }
    if (offset > 0) {
      byte[] tmp = new byte[length];
      System.arraycopy(data, offset, tmp, 0, length);
      data = tmp;
      offset = 0;
    }
    return new ClobOutputStream(this, (int) (pos - 1) + offset);
  }

  /**
   * This method frees the <code>Clob</code> object and releases the resources that it holds. The
   * object is invalid once the <code>free</code> method is called.
   *
   * <p>After <code>free</code> has been called, any attempt to invoke a method other than <code>
   * free</code> will result in a <code>SQLException</code> being thrown. If <code>free</code> is
   * called multiple times, the subsequent calls to <code>free</code> are treated as a no-op.
   */
  public void free() {
    this.data = new byte[0];
    this.offset = 0;
    this.length = 0;
  }

  static class ClobOutputStream extends OutputStream {
    private final SingleStoreClob clob;
    private int pos;

    public ClobOutputStream(SingleStoreClob clob, int pos) {
      this.clob = clob;
      this.pos = pos;
    }

    @Override
    public void write(int bit) {

      if (this.pos >= clob.length) {
        byte[] tmp = new byte[2 * clob.length + 1];
        System.arraycopy(clob.data, clob.offset, tmp, 0, clob.length);
        clob.data = tmp;
        pos -= clob.offset;
        clob.offset = 0;
        clob.length++;
      }
      clob.data[pos++] = (byte) bit;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
      if (off < 0) {
        throw new IOException("Invalid offset " + off);
      }
      if (len < 0) {
        throw new IOException("Invalid len " + len);
      }
      int realLen = Math.min(buf.length - off, len);
      if (pos + realLen >= clob.length) {
        int newLen = 2 * clob.length + realLen;
        byte[] tmp = new byte[newLen];
        System.arraycopy(clob.data, clob.offset, tmp, 0, clob.length);
        clob.data = tmp;
        pos -= clob.offset;
        clob.offset = 0;
        clob.length = pos + realLen;
      }
      System.arraycopy(buf, off, clob.data, pos, realLen);
      pos += realLen;
    }

    @Override
    public void write(byte[] buf) throws IOException {
      write(buf, 0, buf.length);
    }
  }
}
