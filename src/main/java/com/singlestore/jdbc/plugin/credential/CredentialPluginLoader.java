// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential;

import com.singlestore.jdbc.Driver;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import java.sql.SQLException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provider to handle plugin authentication. This can allow library users to override our default
 * Authentication provider.
 */
public final class CredentialPluginLoader {

  private static final ServiceLoader<CredentialPlugin> loader =
      ServiceLoader.load(CredentialPlugin.class, Driver.class.getClassLoader());

  private static final Map<String, CredentialPlugin> loadedPlugins = new ConcurrentHashMap<>();

  /**
   * Get current Identity plugin according to option `identityType`.
   *
   * @param type identity plugin type
   * @return identity plugin
   * @throws SQLException if no identity plugin found with this type is in classpath
   */
  public static CredentialPlugin get(String type) throws SQLException {
    if (type == null) return null;

    CredentialPlugin plugin = loadedPlugins.get(type);
    if (plugin != null) {
      return plugin;
    }

    synchronized (loader) {
      for (CredentialPlugin implClass : loader) {
        String pluginType = implClass.type();
        loadedPlugins.putIfAbsent(pluginType, implClass);

        if (type.equals(pluginType)) {
          return implClass;
        }
      }
    }
    throw new IllegalArgumentException(
        "No identity plugin registered with the type \"" + type + "\".");
  }
}
