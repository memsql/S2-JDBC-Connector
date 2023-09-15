// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.Parameter;
import com.singlestore.jdbc.client.util.Parameters;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.plugin.codec.ByteArrayCodec;
import com.singlestore.jdbc.util.ClientParser;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public final class QueryWithParametersPacket implements RedoableClientMessage {

  private final String preSqlCmd;
  private final ClientParser parser;
  private Parameters parameters;
  private InputStream localInfileInputStream;

  /**
   * Constructor
   *
   * @param preSqlCmd additional pre command
   * @param parser command parser result
   * @param parameters parameters
   */
  public QueryWithParametersPacket(
      String preSqlCmd,
      ClientParser parser,
      Parameters parameters,
      InputStream localInfileInputStream) {
    this.preSqlCmd = preSqlCmd;
    this.parser = parser;
    this.parameters = parameters;
    this.localInfileInputStream = localInfileInputStream;
  }

  @Override
  public void ensureReplayable(Context context) throws IOException, SQLException {
    int parameterCount = parameters.size();
    for (int i = 0; i < parameterCount; i++) {
      Parameter p = parameters.get(i);
      if (!p.isNull() && p.canEncodeLongData()) {
        this.parameters.set(
            i, new com.singlestore.jdbc.codec.Parameter<>(ByteArrayCodec.INSTANCE, p.encodeData()));
      }
    }
  }

  public void saveParameters() {
    this.parameters = this.parameters.clone();
  }

  @Override
  public int encode(Writer encoder, Context context) throws IOException, SQLException {
    encoder.initPacket();
    encoder.writeByte(0x03);
    if (preSqlCmd != null) encoder.writeAscii(preSqlCmd);
    if (parser.getParamPositions().size() == 0) {
      encoder.writeBytes(parser.getQuery());
    } else {
      int pos = 0;
      int paramPos;
      for (int i = 0; i < parser.getParamPositions().size(); i++) {
        paramPos = parser.getParamPositions().get(i);
        encoder.writeBytes(parser.getQuery(), pos, paramPos - pos);
        pos = paramPos + 1;
        parameters.get(i).encodeText(encoder, context);
      }
      encoder.writeBytes(parser.getQuery(), pos, parser.getQuery().length - pos);
    }
    encoder.flush();
    return 1;
  }

  public int batchUpdateLength() {
    return 1;
  }

  public boolean validateLocalFileName(String fileName, Context context) {
    return ClientMessage.validateLocalFileName(parser.getSql(), parameters, fileName, context);
  }

  public InputStream getLocalInfileInputStream() {
    return localInfileInputStream;
  }

  @Override
  public String description() {
    return parser.getSql();
  }
}
