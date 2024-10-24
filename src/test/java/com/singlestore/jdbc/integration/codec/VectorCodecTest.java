// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.SingleStoreBlob;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.util.VectorType;
import com.singlestore.jdbc.type.Vector;
import com.singlestore.jdbc.unit.util.VectorDataUtilsTest;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class VectorCodecTest extends CommonCodecTest {

  private static final String F_VECTOR_TABLE_NAME = "VectorFloatCodec";
  private static final String I_VECTOR_TABLE_NAME = "VectorIntCodec";
  private static final List<Vector> I_VECTOR_VALUES = new ArrayList<>();
  private static final List<Vector> F_VECTOR_VALUES = new ArrayList<>();

  private static Vector generateVector(VectorType type, int length) {
    Number[] arr;
    Supplier<Number> supplier;
    final Random random = new Random();
    switch (type) {
      case I8:
        supplier = () -> (byte) random.nextInt(Byte.MAX_VALUE);
        arr = new Byte[length];
        break;
      case I16:
        supplier = () -> (short) random.nextInt(Short.MAX_VALUE);
        arr = new Short[length];
        break;
      case I32:
        supplier = () -> random.nextInt(Integer.MAX_VALUE);
        arr = new Integer[length];
        break;
      case I64:
        supplier = random::nextLong;
        arr = new Long[length];
        break;
      case F32:
        supplier = () -> (float) random.nextFloat();
        arr = new Float[length];
        break;
      case F64:
        supplier = random::nextDouble;
        arr = new Double[length];
        break;
      default:
        throw new IllegalArgumentException(type + " is not supported.");
    }
    for (int i = 0; i < length; i++) {
      arr[i] = supplier.get();
    }
    String strVal = Arrays.toString(arr).replace(" ", "");
    return Vector.fromData(strVal.getBytes(StandardCharsets.UTF_8), length, type.getType(), false);
  }

  private static Vector convertToBinary(Vector vector) {

    switch (vector.getType()) {
      case INT8_VECTOR:
        return Vector.fromData(vector.toByteArray(), vector.getLength(), vector.getType(), true);
      case INT16_VECTOR:
        return Vector.fromData(
            VectorDataUtilsTest.encodeShortArray(vector.toShortArray()),
            vector.getLength(),
            vector.getType(),
            true);
      case INT32_VECTOR:
        return Vector.fromData(
            VectorDataUtilsTest.encodeIntArray(vector.toIntArray()),
            vector.getLength(),
            vector.getType(),
            true);
      case INT64_VECTOR:
        return Vector.fromData(
            VectorDataUtilsTest.encodeLongArray(vector.toLongArray()),
            vector.getLength(),
            vector.getType(),
            true);
      case FLOAT32_VECTOR:
        return Vector.fromData(
            VectorDataUtilsTest.encodeFloatArray(vector.toFloatArray()),
            vector.getLength(),
            vector.getType(),
            true);
      case FLOAT64_VECTOR:
        return Vector.fromData(
            VectorDataUtilsTest.encodeDoubleArray(vector.toDoubleArray()),
            vector.getLength(),
            vector.getType(),
            true);
      default:
        throw new IllegalArgumentException(vector.getType() + " is not supported.");
    }
  }

  @AfterAll
  public static void drop() throws SQLException {
    Assumptions.assumeTrue(minVersion(8, 7, 1));
    Statement stmt = sharedConn.createStatement();
    stmt.execute(String.format("DROP TABLE IF EXISTS %s", F_VECTOR_TABLE_NAME));
    stmt.execute(String.format("DROP TABLE IF EXISTS %s", I_VECTOR_TABLE_NAME));
  }

  @BeforeAll
  public static void beforeAllData() throws Exception {
    drop();
    F_VECTOR_VALUES.add(generateVector(VectorType.F32, 3));
    F_VECTOR_VALUES.add(generateVector(VectorType.F32, 4));
    F_VECTOR_VALUES.add(generateVector(VectorType.F64, 2));

    I_VECTOR_VALUES.add(generateVector(VectorType.I8, 3));
    I_VECTOR_VALUES.add(generateVector(VectorType.I16, 4));
    I_VECTOR_VALUES.add(generateVector(VectorType.I32, 3));
    I_VECTOR_VALUES.add(generateVector(VectorType.I64, 2));

    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        String.format(
            "CREATE TABLE %s (t1 VECTOR(3), t2 VECTOR(4, F32), t3 VECTOR(2, F64), t4 VECTOR(10), id INT)",
            F_VECTOR_TABLE_NAME));
    stmt.execute(
        String.format(
            "CREATE TABLE %s (t1 VECTOR(3, I8), t2 VECTOR(4, I16), t3 VECTOR(3, I32), t4 VECTOR(2, I64), id INT)",
            I_VECTOR_TABLE_NAME));
    stmt.execute(
        String.format(
            "INSERT INTO %s VALUES ('%s', '%s', '%s', null, 1)",
            F_VECTOR_TABLE_NAME,
            F_VECTOR_VALUES.get(0).stringValue(),
            F_VECTOR_VALUES.get(1).stringValue(),
            F_VECTOR_VALUES.get(2).stringValue()));
    stmt.execute(
        String.format(
            "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s', 1)",
            I_VECTOR_TABLE_NAME,
            I_VECTOR_VALUES.get(0).stringValue(),
            I_VECTOR_VALUES.get(1).stringValue(),
            I_VECTOR_VALUES.get(2).stringValue(),
            I_VECTOR_VALUES.get(3).stringValue()));
    stmt.execute(
        String.format(
            "INSERT INTO %s VALUES ('%s', '%s', '%s', null, 2)",
            I_VECTOR_TABLE_NAME,
            I_VECTOR_VALUES.get(0).stringValue(),
            I_VECTOR_VALUES.get(1).stringValue(),
            I_VECTOR_VALUES.get(2).stringValue()));
  }

  private ResultSet get(Connection con, String table) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            String.format(
                "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from %s ORDER BY id",
                table));
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare(Connection con, String table) throws SQLException {
    PreparedStatement preparedStatement =
        con.prepareStatement(
            String.format(
                "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from %s"
                    + " WHERE 1 > ? ORDER BY id",
                table));
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    assertTrue(rs.next());
    return rs;
  }

  @Test
  public void getObject() throws SQLException {
    try (Connection connection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON");
        Connection binaryConnection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=BINARY")) {
      getIntVectorObject(get(connection, I_VECTOR_TABLE_NAME), false);
      getIntVectorObject(get(binaryConnection, I_VECTOR_TABLE_NAME), true);
      getFloatVectorAsJsonObject(get(connection, F_VECTOR_TABLE_NAME));
      getFloatVectorAsJsonObject(get(binaryConnection, F_VECTOR_TABLE_NAME));
    }
  }

  @Test
  public void getObjectPrepare() throws SQLException {
    try (Connection clientPrepConnection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON");
        Connection clientPrepBinaryConnection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=BINARY");
        Connection serverPrepConnection =
            createCon(
                "enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON&useServerPrepStmts=true")) {
      getIntVectorObject(getPrepare(clientPrepConnection, I_VECTOR_TABLE_NAME), false);
      getIntVectorObject(getPrepare(clientPrepBinaryConnection, I_VECTOR_TABLE_NAME), true);
      getIntVectorObject(getPrepare(serverPrepConnection, I_VECTOR_TABLE_NAME), false);

      getFloatVectorAsJsonObject(getPrepare(clientPrepConnection, F_VECTOR_TABLE_NAME));
      getFloatVectorAsJsonObject(getPrepare(serverPrepConnection, F_VECTOR_TABLE_NAME));
    }
  }

  public void getIntVectorObject(ResultSet rs, boolean isBinary) throws SQLException {
    Vector vector1 = (Vector) rs.getObject(1);
    Vector expectedVector1 = I_VECTOR_VALUES.get(0);

    assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);
    assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray());
    assertArrayEquals(expectedVector1.toDoubleArray(), vector1.toDoubleArray());
    assertArrayEquals(expectedVector1.toIntArray(), vector1.toIntArray());
    assertArrayEquals(expectedVector1.toShortArray(), vector1.toShortArray());
    assertArrayEquals(expectedVector1.toLongArray(), vector1.toLongArray());
    assertArrayEquals(expectedVector1.toStringArray(), vector1.toStringArray());
    assertFalse(rs.wasNull());

    if (!isBinary) {
      assertEquals(expectedVector1.stringValue(), rs.getString(1));
    }

    vector1 = (Vector) rs.getObject(2);
    expectedVector1 = I_VECTOR_VALUES.get(1);
    assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);

    vector1 = (Vector) rs.getObject("t2alias");
    assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);
    assertFalse(rs.wasNull());

    vector1 = rs.getObject(3, Vector.class);
    expectedVector1 = I_VECTOR_VALUES.get(2);
    assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);
    assertFalse(rs.wasNull());

    vector1 = (Vector) rs.getObject(4);
    expectedVector1 = I_VECTOR_VALUES.get(3);
    assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);
  }

  public void getFloatVectorAsJsonObject(ResultSet rs) throws SQLException {
    Vector vector1 = (Vector) rs.getObject(1);
    Vector expectedVector1 = F_VECTOR_VALUES.get(0);
    assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray(), 0.0000001f);
    assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);
    assertEquals(expectedVector1.getType(), vector1.getType());
    assertFalse(rs.wasNull());

    vector1 = (Vector) rs.getObject(2);
    expectedVector1 = F_VECTOR_VALUES.get(1);
    assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray(), 0.0000001f);
    assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);

    vector1 = (Vector) rs.getObject("t2alias");
    assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray(), 0.0000001f);
    assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);

    vector1 = rs.getObject(3, Vector.class);
    expectedVector1 = F_VECTOR_VALUES.get(2);
    assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);

    vector1 = (Vector) rs.getObject(4);
    assertNull(vector1);
  }

  @Test
  public void getObjectType() throws Exception {
    try (Connection connection =
        createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON")) {
      getObjectType(get(connection, I_VECTOR_TABLE_NAME));
    }
  }

  @Test
  public void getObjectTypePrepare() throws Exception {
    try (Connection clientPrepConnection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON");
        Connection serverPrepConnection =
            createCon(
                "enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON&useServerPrepStmts=true")) {
      getObjectType(getPrepare(clientPrepConnection, I_VECTOR_TABLE_NAME));
      getObjectType(getPrepare(serverPrepConnection, I_VECTOR_TABLE_NAME));
    }
  }

  public void getObjectType(ResultSet rs) throws Exception {
    assertTrue(rs.next());
    testErrObject(rs, Integer.class);
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testErrObject(rs, Byte.class);
    testObject(rs, byte[].class, I_VECTOR_VALUES.get(0).getValues());
    testObject(rs, InputStream.class, new ByteArrayInputStream(I_VECTOR_VALUES.get(0).getValues()));
    testObject(rs, Blob.class, new SingleStoreBlob(I_VECTOR_VALUES.get(0).getValues()));
    testObject(rs, String.class, I_VECTOR_VALUES.get(0).stringValue());
    testObject(rs, Vector.class, I_VECTOR_VALUES.get(0));
  }

  @Test
  public void getMetaData() throws SQLException {
    try (com.singlestore.jdbc.Connection connection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON");
        com.singlestore.jdbc.Connection binaryVectorConnection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=BINARY")) {
      getMetaData(connection);
      getMetaData(binaryVectorConnection);
    }
  }

  private void getMetaData(com.singlestore.jdbc.Connection con) throws SQLException {
    ResultSet rs = getPrepare(con, I_VECTOR_TABLE_NAME);
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("VECTOR", meta.getColumnTypeName(1));
    assertEquals(con.getCatalog(), meta.getCatalogName(1));
    assertEquals(Vector.class.getName(), meta.getColumnClassName(1));

    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.OTHER, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));

    assertEquals("VECTOR", meta.getColumnTypeName(2));
    assertEquals("VECTOR", meta.getColumnTypeName(3));
    assertEquals("VECTOR", meta.getColumnTypeName(4));

    rs = getPrepare(con, F_VECTOR_TABLE_NAME);
    meta = rs.getMetaData();

    assertEquals("VECTOR", meta.getColumnTypeName(1));
    assertEquals(con.getCatalog(), meta.getCatalogName(1));
    assertEquals(Vector.class.getName(), meta.getColumnClassName(1));

    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.OTHER, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
  }

  @Test
  public void sendParam() throws Exception {
    try (Connection connection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON");
        Connection binaryVectorConnection =
            createCon("enableExtendedDataTypes=true&vectorTypeOutputFormat=BINARY")) {
      sendParam(connection, false);
      sendParam(binaryVectorConnection, true);
    }
  }

  private void sendParam(Connection con, boolean isBinary) throws Exception {
    List<Vector> vectors = new ArrayList<>(4);
    vectors.add(
        isBinary
            ? convertToBinary(generateVector(VectorType.I64, 2))
            : generateVector(VectorType.I64, 2));
    vectors.add(
        isBinary
            ? convertToBinary(generateVector(VectorType.I64, 2))
            : generateVector(VectorType.I64, 2));
    vectors.add(
        isBinary
            ? convertToBinary(generateVector(VectorType.I64, 2))
            : generateVector(VectorType.I64, 2));
    vectors.add(
        isBinary
            ? convertToBinary(generateVector(VectorType.I64, 2))
            : generateVector(VectorType.I64, 2));
    assertEquals(4, vectors.size());
    Vector vector1 = vectors.get(0);
    Vector vector2 = vectors.get(1);
    Vector vector3 = vectors.get(2);
    Vector vector4 = vectors.get(3);
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS VectorCodec2");
    stmt.execute(
        "CREATE TABLE VectorCodec2 (id int not null primary key auto_increment, t1 VECTOR(2, I64))");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO VectorCodec2(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setObject(2, vector1);
      prep.execute();
      prep.setInt(1, 2);
      prep.setObject(2, (Vector) null);
      prep.execute();
      prep.setInt(1, 3);
      prep.setObject(2, vector2);
      prep.addBatch();
      prep.setInt(1, 4);
      prep.setObject(2, vector3);
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM VectorCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertEquals(vector1, rs.getObject(2, Vector.class));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, Vector.class));
    assertTrue(rs.next());
    assertNull(rs.getObject(2, Vector.class));
    rs.updateObject(2, vector4);
    rs.updateRow();
    assertEquals(vector4, rs.getObject(2, Vector.class));

    assertTrue(rs.next());
    assertEquals(vector2, rs.getObject(2, Vector.class));
    assertTrue(rs.next());
    assertEquals(vector3, rs.getObject(2, Vector.class));
  }
}
