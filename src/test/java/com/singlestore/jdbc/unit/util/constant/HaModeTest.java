// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.unit.util.constant;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.impl.ConnectionHelper;
import com.singlestore.jdbc.export.HaMode;
import com.singlestore.jdbc.plugin.authentication.AuthenticationPluginLoader;
import com.singlestore.jdbc.plugin.credential.CredentialPluginLoader;
import com.singlestore.jdbc.plugin.tls.TlsSocketPluginLoader;
import com.singlestore.jdbc.pool.Pools;
import com.singlestore.jdbc.util.CharsetEncodingLength;
import com.singlestore.jdbc.util.NativeSql;
import com.singlestore.jdbc.util.Security;
import com.singlestore.jdbc.util.VersionFactory;
import com.singlestore.jdbc.util.constants.*;
import com.singlestore.jdbc.util.log.LoggerHelper;
import com.singlestore.jdbc.util.log.Loggers;
import com.singlestore.jdbc.util.options.OptionAliases;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HaModeTest {
  @Test
  public void instantiateStaticOnlyClass() {
    Capabilities capabilities = new Capabilities();
    ColumnFlags columnFlags = new ColumnFlags();
    ConnectionState connectionState = new ConnectionState();
    ServerStatus serverStatus = new ServerStatus();
    StateChange stateChange = new StateChange();
    CharsetEncodingLength c = new CharsetEncodingLength();
    NativeSql n = new NativeSql();
    Security s = new Security();
    OptionAliases oa = new OptionAliases();
    CredentialPluginLoader cp = new CredentialPluginLoader();
    AuthenticationPluginLoader ap = new AuthenticationPluginLoader();
    TlsSocketPluginLoader tp = new TlsSocketPluginLoader();
    LoggerHelper lh = new LoggerHelper();
    ConnectionHelper ch = new ConnectionHelper();
    Pools p = new Pools();
    Loggers l = new Loggers();
    VersionFactory vv = new VersionFactory();
  }

  @Test
  public void loadBalanceTest() throws InterruptedException {
    HostAddress host1 = HostAddress.from("1", 3306);
    HostAddress host2 = HostAddress.from("2", 3306);
    HostAddress host3 = HostAddress.from("3", 3306);
    HostAddress host4 = HostAddress.from("4", 3306);
    HostAddress host5 = HostAddress.from("5", 3306);

    List<HostAddress> available = new ArrayList<>();
    available.add(host1);
    available.add(host2);
    available.add(host3);
    available.add(host4);
    available.add(host5);

    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    Map<HostAddress, Integer> res = loopPercReturn(available, denyList);
    Assertions.assertEquals(200, res.get(host1));
    Assertions.assertEquals(200, res.get(host2));
    Assertions.assertEquals(200, res.get(host3));
    Assertions.assertEquals(200, res.get(host4));
    Assertions.assertEquals(200, res.get(host5));

    denyList.putIfAbsent(host1, System.currentTimeMillis() + 1000000);

    res = loopPercReturn(available, denyList);
    Assertions.assertNull(res.get(host1));
    Assertions.assertEquals(250, res.get(host2));
    Assertions.assertEquals(250, res.get(host3));

    denyList.clear();
    denyList.putIfAbsent(host1, System.currentTimeMillis() - 1000000);

    res = loopPercReturn(available, denyList);
    Assertions.assertEquals(200, res.get(host1));
    Assertions.assertEquals(200, res.get(host2));
    Assertions.assertEquals(200, res.get(host3));

    res = loopPercReturn(available, denyList);
    Assertions.assertEquals(200, res.get(host4));
    Assertions.assertEquals(200, res.get(host5));
  }

  @Test
  public void noneTest() {
    HostAddress host1 = HostAddress.from("1", 3306);

    List<HostAddress> available = new ArrayList<>();
    available.add(host1);

    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();

    Optional<HostAddress> res = HaMode.NONE.getAvailableHost(available, denyList);
    Assertions.assertTrue(res.isPresent());
    Assertions.assertEquals(host1, res.get());

    res = HaMode.NONE.getAvailableHost(new ArrayList<>(), denyList);
    Assertions.assertFalse(res.isPresent());
  }

  private Map<HostAddress, Integer> loopPercReturn(
      List<HostAddress> available, ConcurrentMap<HostAddress, Long> denyList) {
    Map<HostAddress, Integer> resMap = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      Optional<HostAddress> res = HaMode.LOADBALANCE.getAvailableHost(available, denyList);
      if (res.isPresent()) {
        if (resMap.containsKey(res.get())) {
          resMap.put(res.get(), resMap.get(res.get()) + 1);
        } else {
          resMap.put(res.get(), 1);
        }
      }
    }
    return resMap;
  }
}
