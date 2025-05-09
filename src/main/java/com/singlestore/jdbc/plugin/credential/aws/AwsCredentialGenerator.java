// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.aws;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.Credential;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.rds.RdsUtilities;

public class AwsCredentialGenerator {

  private final String authenticationToken;
  private final String userName;

  /**
   * AWS Generator.
   *
   * @param nonMappedOptions non standard options
   * @param userName user
   * @param hostAddress current server information
   */
  public AwsCredentialGenerator(
      Properties nonMappedOptions, String userName, HostAddress hostAddress) {
    // Build RDS IAM-auth token generator
    this.userName = userName;
    AwsCredentialsProvider awsCredentialsProvider;
    String accessKeyId = nonMappedOptions.getProperty("accessKeyId");
    String secretKey = nonMappedOptions.getProperty("secretKey");
    String region = nonMappedOptions.getProperty("region");
    if (accessKeyId != null && secretKey != null) {
      awsCredentialsProvider =
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretKey));
    } else {
      awsCredentialsProvider = DefaultCredentialsProvider.builder().build();
    }
    RdsUtilities utilities =
        RdsUtilities.builder()
            .credentialsProvider(awsCredentialsProvider)
            .region(
                region != null
                    ? Region.of(region)
                    : new DefaultAwsRegionProviderChain().getRegion())
            .build();

    this.authenticationToken =
        utilities.generateAuthenticationToken(
            builder -> {
              builder
                  .username(userName)
                  .hostname(hostAddress.host)
                  .port(hostAddress.port)
                  .credentialsProvider(awsCredentialsProvider);
            });
  }

  public Credential getToken() {
    return new Credential(userName, authenticationToken);
  }
}
