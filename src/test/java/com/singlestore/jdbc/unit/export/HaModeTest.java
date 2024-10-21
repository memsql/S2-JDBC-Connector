// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.unit.export;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.export.HaMode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;

public class HaModeTest {
  @Test
  public void getAvailableHostWithoutConnectionNumber() {
    List<HostAddress> hostAddresses = new ArrayList<>();
    hostAddresses.add(HostAddress.from("master1", 3306));
    hostAddresses.add(HostAddress.from("child1", 3306));
    hostAddresses.add(HostAddress.from("child2", 3306));
    hostAddresses.add(HostAddress.from("child3", 3306));
    hostAddresses.add(HostAddress.from("child4", 3306));
    hostAddresses.add(HostAddress.from("child5", 3306));

    HaMode.LOADBALANCE.resetLast();
    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    HostCounter hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost =
          HaMode.LOADBALANCE.getAvailableHost(hostAddresses, denyList);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals(
        "child1:17,child2:17,child3:17,child4:16,child5:16,master1:17", hostCounter.results());

    HaMode.LOADBALANCE.resetLast();
    denyList.put(hostAddresses.get(0), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(1), System.currentTimeMillis() + 1000);

    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost =
          HaMode.LOADBALANCE.getAvailableHost(hostAddresses, denyList);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("child2:20,child3:20,child4:20,child5:20,master1:20", hostCounter.results());

    HaMode.LOADBALANCE.resetLast();
    denyList.clear();
    denyList.put(hostAddresses.get(3), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(4), System.currentTimeMillis() + 1000);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost =
          HaMode.LOADBALANCE.getAvailableHost(hostAddresses, denyList);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("child1:20,child2:20,child3:20,child5:20,master1:20", hostCounter.results());
  }

  @Test
  public void getAvailableHostWithConnectionNumber() {
    List<HostAddress> hostAddresses = new ArrayList<>();

    HostAddress host1 = HostAddress.from("master1", 3306);
    HostAddress host2 = HostAddress.from("master2", 3306);
    HostAddress host3 = HostAddress.from("master3", 3306);
    host1.setThreadsConnected(200);
    host2.setThreadsConnected(150);
    host3.setThreadsConnected(100);
    hostAddresses.add(host1);
    hostAddresses.add(host2);
    hostAddresses.add(host3);

    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    HostCounter hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost =
          HaMode.LOADBALANCE.getAvailableHost(hostAddresses, denyList);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("master2:25,master3:75", hostCounter.results());

    host1.forceThreadsConnected(200, System.currentTimeMillis() - 50000);
    host2.setThreadsConnected(150);
    host3.setThreadsConnected(100);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost =
          HaMode.LOADBALANCE.getAvailableHost(hostAddresses, denyList);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("master1:34,master2:33,master3:33", hostCounter.results());

    denyList.put(hostAddresses.get(0), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(1), System.currentTimeMillis() + 1000);
    host1.setThreadsConnected(150);
    host2.setThreadsConnected(150);
    host3.setThreadsConnected(100);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost =
          HaMode.LOADBALANCE.getAvailableHost(hostAddresses, denyList);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("master1:25,master3:75", hostCounter.results());

    denyList.clear();
    denyList.put(hostAddresses.get(1), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(2), System.currentTimeMillis() + 1000);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost =
          HaMode.LOADBALANCE.getAvailableHost(hostAddresses, denyList);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("master1:38,master2:62", hostCounter.results());
  }

  private static class HostCounter {
    Map<HostAddress, Integer> hosts = new HashMap<>();

    public void add(HostAddress hostAddress, boolean increment) {
      Integer counter = hosts.get(hostAddress);
      if (counter == null) {
        hosts.put(hostAddress, 1);
      } else {
        hosts.replace(hostAddress, counter + 1);
      }
      if (increment) {
        if (hostAddress.getThreadsConnected() != null) {
          hostAddress.forceThreadsConnected(
              hostAddress.getThreadsConnected() + 1, hostAddress.getThreadConnectedTimeout());
        } else {
          hostAddress.forceThreadsConnected(1, System.currentTimeMillis() + 1000);
        }
      }
    }

    public String results() {
      List<String> res = new ArrayList<>();
      for (Map.Entry<HostAddress, Integer> hostEntry : hosts.entrySet()) {
        res.add(hostEntry.getKey().host + ':' + hostEntry.getValue());
      }
      Collections.sort(res);
      return String.join(",", res);
    }
  }
}
