// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.plugin.authentication;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.Driver;
import com.singlestore.jdbc.plugin.AuthenticationPluginFactory;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.ServiceLoader;

public final class AuthenticationPluginLoader {

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @param conf current configuration
   * @return Authentication plugin corresponding to type
   * @throws SQLException if no authentication plugin in classpath have indicated type
   */
  public static AuthenticationPluginFactory get(String type, Configuration conf)
      throws SQLException {

    ServiceLoader<AuthenticationPluginFactory> loader =
        ServiceLoader.load(AuthenticationPluginFactory.class, Driver.class.getClassLoader());

    String[] authList = (conf.restrictedAuth() != null) ? conf.restrictedAuth().split(",") : null;

    for (AuthenticationPluginFactory implClass : loader) {
      if (type.equals(implClass.type())) {
        if (authList == null || Arrays.stream(authList).anyMatch(type::contains)) {
          return implClass;
        } else {
          throw new SQLException(
              String.format(
                  "Client restrict authentication plugin to a limited set of authentication plugin and doesn't permit requested plugin ('%s'). "
                      + "Current list is `restrictedAuth=%s`",
                  type, conf.restrictedAuth()),
              "08004",
              1251);
        }
      }
    }
    throw new SQLException(
        "Client does not support authentication protocol requested by server. "
            + "plugin type was = '"
            + type
            + "'",
        "08004",
        1251);
  }
}
