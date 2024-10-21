// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.export;

import com.singlestore.jdbc.HostAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/** Failover (High-availability) mode */
public enum HaMode {
  SEQUENTIAL("sequential") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
      return getAvailableHostInOrder(hostAddresses, denyList);
    }
  },
  /**
   * load-balance: driver will connect to any host using round-robin, permitting balancing
   * connections
   */
  LOADBALANCE("load-balance") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
      HostAddress hostWithLessConnection = getHostWithLessConnections(hostAddresses, denyList);
      if (hostWithLessConnection != null) return Optional.of(hostWithLessConnection);
      return HaMode.getAvailableRoundRobinHost(this, hostAddresses, denyList);
    }
  },
  /** no ha-mode. Connect to first host only */
  NONE("") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
      return hostAddresses.isEmpty() ? Optional.empty() : Optional.of(hostAddresses.get(0));
    }
  };

  private final String value;
  private HostAddress lastRoundRobinHost = null;

  HaMode(String value) {
    this.value = value;
  }

  /**
   * Get HAMode from values or aliases
   *
   * @param value value or alias
   * @return HaMode if corresponding mode is found
   */
  public static HaMode from(String value) {
    for (HaMode haMode : values()) {
      if (haMode.value.equalsIgnoreCase(value) || haMode.name().equalsIgnoreCase(value)) {
        return haMode;
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for HaMode", value));
  }

  /**
   * Get first available host not in blacklist.
   *
   * @param hostAddresses hosts
   * @param denyList blacklist
   * @return list without denied hosts
   */
  public static Optional<HostAddress> getAvailableHostInOrder(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
    // use in order not blacklisted server
    HostAddress hostAddress;
    for (HostAddress address : hostAddresses) {
      hostAddress = address;
      if (!denyList.containsKey(hostAddress)) return Optional.of(hostAddress);
      if (denyList.get(hostAddress) < System.currentTimeMillis()) {
        // timeout reached
        denyList.remove(hostAddress);
        return Optional.of(hostAddress);
      }
    }
    return Optional.empty();
  }

  /**
   * If all hosts not blacklisted connection number are known, choose the host with the less
   * connections.
   *
   * @param hostAddresses host addresses
   * @param denyList blacklist
   * @return the host with less connection, or null if unknown.
   */
  public static HostAddress getHostWithLessConnections(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
    long currentTime = System.currentTimeMillis();
    HostAddress hostAddressWithLessConnections = null;

    for (HostAddress hostAddress : hostAddresses) {
      if (denyList.containsKey(hostAddress)) {
        // take in account denied server that have reached denied timeout
        if (denyList.get(hostAddress) > System.currentTimeMillis()) {
          continue;
        } else {
          denyList.remove(hostAddress);
        }
      }

      // All host must have recently been connected
      if (hostAddress.getThreadConnectedTimeout() == null
          || hostAddress.getThreadConnectedTimeout() < currentTime) {
        return null;
      }
      if (hostAddressWithLessConnections == null
          || hostAddressWithLessConnections.getThreadsConnected()
              > hostAddress.getThreadsConnected()) {
        hostAddressWithLessConnections = hostAddress;
      }
    }
    return hostAddressWithLessConnections;
  }

  /**
   * Get first available host in RoundRobin Order and not in blacklist.
   *
   * @param haMode current haMode
   * @param hostAddresses hosts
   * @param denyList blacklist
   * @return list without denied hosts
   */
  public static Optional<HostAddress> getAvailableRoundRobinHost(
      HaMode haMode, List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
    HostAddress lastChosenHost = haMode.lastRoundRobinHost;

    List<HostAddress> loopList;
    if (lastChosenHost == null) {
      loopList = hostAddresses;
    } else {
      int lastChosenIndex = hostAddresses.indexOf(lastChosenHost);
      loopList = new ArrayList<>();
      loopList.addAll(hostAddresses.subList(lastChosenIndex + 1, hostAddresses.size()));
      loopList.addAll(hostAddresses.subList(0, lastChosenIndex + 1));
    }

    for (HostAddress hostAddress : loopList) {
      if (denyList.containsKey(hostAddress)) {
        // take in account denied server that have reached denied timeout
        if (denyList.get(hostAddress) > System.currentTimeMillis()) {
          continue;
        } else {
          denyList.remove(hostAddress);
        }
      }
      haMode.lastRoundRobinHost = hostAddress;
      return Optional.of(hostAddress);
    }
    return Optional.empty();
  }

  /** For testing purpose only */
  public void resetLast() {
    lastRoundRobinHost = null;
  }

  /**
   * List of hosts without blacklist entries, ordered according to HA mode
   *
   * @param hostAddresses hosts
   * @param denyList hosts temporary denied
   * @return list without denied hosts
   */
  public abstract Optional<HostAddress> getAvailableHost(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList);
}
