package com.singlestore.jdbc.plugin.credential.browser.keyring;

import com.singlestore.jdbc.plugin.credential.browser.ExpiringCredential;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.freedesktop.secret.simple.SimpleCollection;

public class LinuxKeyring implements Keyring {
  private static final Map<String, String> ATTRIBUTES =
      Collections.singletonMap("application", "singlestore-jdbc");
  final SimpleCollection collection;

  public LinuxKeyring() throws IOException {
    try {
      collection = new SimpleCollection();
    } catch (IOException e) {
      throw new IOException("Error while accessing GNOME Keyring", e);
    }
  }

  @Override
  public ExpiringCredential getCredential() throws SQLException {
    String entryPath = getExistingEntry();
    if (entryPath == null) {
      return null;
    }
    try {
      return Keyring.fromBlob(String.valueOf(collection.getSecret(entryPath)));
    } catch (IOException e) {
      throw new SQLException("Error while parsing cached token from the GNOME Keyring", e);
    }
  }

  @Override
  public void setCredential(ExpiringCredential cred) throws SQLException {
    String entryPath = getExistingEntry();
    String credBlob = Keyring.makeBlob(cred);
    if (entryPath == null) {
      entryPath = collection.createItem(STORAGE_KEY, credBlob);
    }
    collection.updateItem(entryPath, STORAGE_KEY, credBlob, ATTRIBUTES);
  }

  private String getExistingEntry() throws SQLException {
    String foundPath = null;
    List<String> entires = collection.getItems(ATTRIBUTES);
    if (entires != null) {
      for (String entry : collection.getItems(ATTRIBUTES)) {
        if (collection.getLabel(entry).equals(STORAGE_KEY)) {
          if (foundPath != null) {
            throw new SQLException(
                "Found multiple keychain entries matching \"" + STORAGE_KEY + "\"");
          }
          foundPath = entry;
        }
        ;
      }
    }
    return foundPath;
  }
}
