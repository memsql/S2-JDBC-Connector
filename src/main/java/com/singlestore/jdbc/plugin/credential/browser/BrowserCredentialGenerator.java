package com.singlestore.jdbc.plugin.credential.browser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.singlestore.jdbc.plugin.credential.Credential;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;

public class BrowserCredentialGenerator {

  private final File authHelperBin;

  public BrowserCredentialGenerator(Properties nonMappedOptions) throws SQLException {
    String path = nonMappedOptions.getProperty("authHelperPath");
    if (path == null) {
      // TODO: this will depend on how we distribute auth helper
      path = "changeme";
    }

    authHelperBin = new File(path);
    if (!authHelperBin.exists()) {
      throw new SQLException(
          "Identity plugin 'BROWSER' is used without having Auth Helper at \""
              + path
              + "\". Please install Auth Helper or provide a path to an existing one via authHelperPath option.");
    }
    if (!authHelperBin.canExecute()) {
      throw new SQLException(
          "Identity plugin 'BROWSER' is used but Auth Helper at \""
              + path
              + "\" is not executable. Please make sure that the JVM has the permission to execute this file.");
    }
  }

  public ExpiringCredential getCredential(String email) throws SQLException {
    Runtime rt = Runtime.getRuntime();
    String[] commands;
    if (email != null) {
      commands = new String[] {authHelperBin.getAbsolutePath(), email};
    } else {
      commands = new String[] {authHelperBin.getAbsolutePath()};
    }

    Process proc;
    try {
      proc = rt.exec(commands);
      proc.waitFor();
    } catch (IOException e) {
      throw new SQLException(
          "Could not execute Auth Helper at "
              + authHelperBin.getPath()
              + "when using identity plugin 'BROWSER'",
          e);
    } catch (InterruptedException e) {
      throw new SQLException(e);
    }

    String stdOut =
        new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));

    String stdError =
        new BufferedReader(new InputStreamReader(proc.getErrorStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));

    if (proc.exitValue() != 0) {
      throw new SQLException(
          "Auth Helper returned an error when using identity plugin 'BROWSER'."
              + "\nStdout:\n"
              + stdOut
              + "\nStderr:\n"
              + stdError);
    }

    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<HashMap<String, String>> mapType =
        new TypeReference<HashMap<String, String>>() {};
    HashMap<String, String> outMap;
    try {
      outMap = objectMapper.readValue(stdOut, mapType);
    } catch (IOException e) {
      throw new SQLException(
          "Could not parse output from Auth Helper when using identity plugin 'BROWSER'."
              + "\nStdout:\n"
              + stdOut
              + "\nStderr:\n"
              + stdError);
    }

    return new ExpiringCredential(
        new Credential(outMap.get("username"), outMap.get("token")),
        outMap.get("email"),
        Instant.parse(outMap.get("expiration")));
  }
}
