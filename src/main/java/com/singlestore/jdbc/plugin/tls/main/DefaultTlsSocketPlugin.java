// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.plugin.tls.main;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.tls.HostnameVerifier;
import com.singlestore.jdbc.client.tls.SingleStoreX509KeyManager;
import com.singlestore.jdbc.client.tls.SingleStoreX509TrustingManager;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.plugin.TlsSocketPlugin;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class DefaultTlsSocketPlugin implements TlsSocketPlugin {

  private static KeyManager loadClientCerts(
      String keyStoreUrl,
      String keyStorePassword,
      String keyPassword,
      String storeType,
      ExceptionFactory exceptionFactory)
      throws SQLException {

    try {
      try (InputStream inStream = loadFromUrl(keyStoreUrl)) {
        char[] keyStorePasswordChars =
            keyStorePassword == null
                ? null
                : (keyStorePassword.equals("")) ? null : keyStorePassword.toCharArray();
        char[] keyStoreChars =
            (keyPassword == null)
                ? keyStorePasswordChars
                : (keyPassword.equals("")) ? null : keyPassword.toCharArray();
        KeyStore ks =
            KeyStore.getInstance(storeType != null ? storeType : KeyStore.getDefaultType());
        ks.load(inStream, keyStorePasswordChars);
        return new SingleStoreX509KeyManager(ks, keyStoreChars);
      }
    } catch (IOException | GeneralSecurityException ex) {
      throw exceptionFactory.create(
          "Failed to read keyStore file. Option keyStore=" + keyStoreUrl, "08000", ex);
    }
  }

  private static InputStream loadFromUrl(String keyStoreUrl) throws FileNotFoundException {
    try {
      return new URI(keyStoreUrl).toURL().openStream();
    } catch (Exception exception) {
      return new FileInputStream(keyStoreUrl);
    }
  }

  private static InputStream getInputStreamFromPath(String path) throws IOException {
    try {
      return new URI(path).toURL().openStream();
    } catch (Exception e) {
      if (path.startsWith("-----")) {
        return new ByteArrayInputStream(path.getBytes());
      } else {
        File f = new File(path);
        if (f.exists() && !f.isDirectory()) {
          return f.toURI().toURL().openStream();
        }
        throw new IOException(
            String.format("File not found for option `serverSslCert` (value: '%s')", path), e);
      }
    }
  }

  @Override
  public String type() {
    return "DEFAULT";
  }

  @Override
  public TrustManager[] getTrustManager(Configuration conf, ExceptionFactory exceptionFactory)
      throws SQLException {
    TrustManager[] trustManager = null;
    if (conf.sslMode() == SslMode.TRUST) {
      trustManager = new X509TrustManager[] {new SingleStoreX509TrustingManager()};
    } else {
      if (conf.serverSslCert() != null || conf.trustStore() != null) {
        KeyStore ks;
        try {
          ks =
              KeyStore.getInstance(
                  conf.trustStoreType() != null
                      ? conf.trustStoreType()
                      : KeyStore.getDefaultType());
        } catch (GeneralSecurityException generalSecurityEx) {
          throw exceptionFactory.create(
              "Failed to create keystore instance", "08000", generalSecurityEx);
        }
        if (conf.trustStore() != null) {
          InputStream inStream;
          try {
            inStream = loadFromUrl(conf.trustStore());
          } catch (IOException ioexception) {
            try {
              inStream = new FileInputStream(conf.trustStore());
            } catch (FileNotFoundException fileNotFoundEx) {
              throw new SQLException(
                  "Failed to find trustStore file. trustStore=" + conf.trustStore(),
                  "08000",
                  fileNotFoundEx);
            }
          }
          try {
            ks.load(
                inStream,
                conf.trustStorePassword() == null ? null : conf.trustStorePassword().toCharArray());
          } catch (IOException | NoSuchAlgorithmException | CertificateException ioEx) {
            throw exceptionFactory.create("Failed load keyStore", "08000", ioEx);
          } finally {
            try {
              inStream.close();
            } catch (IOException e) {
              // eat
            }
          }
        } else {
          try (InputStream inStream = getInputStreamFromPath(conf.serverSslCert())) {
            // generate a keyStore from the provided cert

            // Note: KeyStore requires it be loaded even if you don't load anything into it
            // (will be initialized with "javax.net.ssl.trustStore") values.
            ks.load(null);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<? extends Certificate> caList = cf.generateCertificates(inStream);
            for (Certificate ca : caList) {
              ks.setCertificateEntry(UUID.randomUUID().toString(), ca);
            }

          } catch (IOException ioEx) {
            throw exceptionFactory.create("Failed load keyStore", "08000", ioEx);
          } catch (GeneralSecurityException generalSecurityEx) {
            throw exceptionFactory.create(
                "Failed to store certificate from serverSslCert into a keyStore",
                "08000",
                generalSecurityEx);
          }
        }

        try {
          TrustManagerFactory tmf =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(ks);
          for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
              trustManager = new X509TrustManager[] {(X509TrustManager) tm};
              break;
            }
          }

          for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
              trustManager = new X509TrustManager[] {(X509TrustManager) tm};
              break;
            }
          }

        } catch (GeneralSecurityException generalSecurityEx) {
          throw exceptionFactory.create(
              "Failed to load certificates from serverSslCert/trustStore",
              "08000",
              generalSecurityEx);
        }
        if (trustManager == null) {
          throw new SQLException("No X509TrustManager found");
        }
      }
    }
    return trustManager;
  }

  @Override
  public KeyManager[] getKeyManager(Configuration conf, ExceptionFactory exceptionFactory)
      throws SQLException {
    KeyManager[] keyManager = null;
    if (conf.keyStore() != null) {
      keyManager =
          new KeyManager[] {
            loadClientCerts(
                conf.keyStore(),
                conf.keyStorePassword(),
                conf.keyPassword(),
                conf.keyStoreType(),
                exceptionFactory)
          };
    } else {
      String keyStore = System.getProperty("javax.net.ssl.keyStore");
      String keyStorePassword =
          System.getProperty("javax.net.ssl.keyStorePassword", conf.keyStorePassword());
      String keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", conf.keyStoreType());
      if (keyStore != null) {
        try {
          keyManager =
              new KeyManager[] {
                loadClientCerts(
                    keyStore, keyStorePassword, keyStorePassword, keyStoreType, exceptionFactory)
              };
        } catch (SQLException queryException) {
          keyManager = null;
          Loggers.getLogger(DefaultTlsSocketPlugin.class)
              .error("Error loading key manager from system properties", queryException);
        }
      }
    }
    return keyManager;
  }

  @Override
  public void verify(String host, SSLSession session, long serverThreadId) throws SSLException {
    try {
      Certificate[] certs = session.getPeerCertificates();
      X509Certificate cert = (X509Certificate) certs[0];
      HostnameVerifier.verify(host, cert, serverThreadId);
    } catch (SSLException ex) {
      Loggers.getLogger(DefaultTlsSocketPlugin.class).info(ex.getMessage(), ex);
      throw ex;
    }
  }
}
