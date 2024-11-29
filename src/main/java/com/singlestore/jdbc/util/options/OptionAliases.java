// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.util.options;

import java.util.HashMap;
import java.util.Map;

public final class OptionAliases {

  public static final Map<String, String> OPTIONS_ALIASES;

  static {
    OPTIONS_ALIASES = new HashMap<>();
    OPTIONS_ALIASES.put("clientcertificatekeystoreurl", "keyStore");
    OPTIONS_ALIASES.put("clientcertificatekeystorepassword", "keyStorePassword");
    OPTIONS_ALIASES.put("clientcertificatekeystoretype", "keyStoreType");
    OPTIONS_ALIASES.put("trustcertificatekeystoreurl", "trustStore");
    OPTIONS_ALIASES.put("trustcertificatekeystorepassword", "keyStorePassword");
    OPTIONS_ALIASES.put("trustcertificatekeystoretype", "trustStoreType");
  }
}
