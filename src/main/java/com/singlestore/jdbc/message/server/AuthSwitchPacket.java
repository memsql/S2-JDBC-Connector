// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.message.ServerMessage;
import java.util.Arrays;

public class AuthSwitchPacket implements ServerMessage {

  private final String plugin;
  private final byte[] seed;

  /**
   * Authentication switch constructor
   *
   * @param plugin plugin requested
   * @param seed plugin seed
   */
  public AuthSwitchPacket(String plugin, byte[] seed) {
    this.plugin = plugin;
    this.seed = seed;
  }

  /**
   * Decode an AUTH_SWITCH_PACKET from a MYSQL packet.
   *
   * @param buf packet
   * @return Authentication switch packet.
   */
  public static AuthSwitchPacket decode(ReadableByteBuf buf) {
    buf.skip(1);
    String plugin = buf.readStringNullEnd();

    byte[] seed = new byte[buf.readableBytes()];
    buf.readBytes(seed);
    return new AuthSwitchPacket(plugin, seed);
  }

  /**
   * Get authentication switch plugin information
   *
   * @return plugin
   */
  public String getPlugin() {
    return plugin;
  }

  /**
   * Get authentication switch seed information
   *
   * @return seed
   */
  public byte[] getSeed() {
    return seed;
  }

  /**
   * Get truncated seed (seed without ending 0x00 byte)
   *
   * @param seed connection seed
   * @return truncated seed
   */
  public static byte[] getTruncatedSeed(byte[] seed) {
    return (seed.length > 0) ? Arrays.copyOfRange(seed, 0, seed.length - 1) : new byte[0];
  }
}
