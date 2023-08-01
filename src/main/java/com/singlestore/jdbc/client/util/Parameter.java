// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2023 SingleStore, Inc.

package com.singlestore.jdbc.client.util;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import java.io.IOException;
import java.sql.SQLException;

/** Parameter */
public interface Parameter {

  void encodeText(Writer encoder, Context context) throws IOException, SQLException;

  void encodeBinary(Writer encoder) throws IOException, SQLException;

  void encodeLongData(Writer encoder) throws IOException, SQLException;

  byte[] encodeData() throws IOException, SQLException;

  boolean canEncodeLongData();

  int getBinaryEncodeType();

  boolean isNull();
}
