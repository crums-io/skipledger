/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql.config;


import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.sldg.src.SaltScheme;
import io.crums.util.json.simple.JSONObject;

/**
 * Tests for config JSON parsers: LedgerDef, SaltSeed, DbCredentials,
 * DbConnection, SaltSchemeParser.
 */
public class ConfigTest {


  @Test
  public void testLedgerDefRoundtrip() {
    String sizeQ = "SELECT MAX(id) FROM ledger";
    String rowQ = "SELECT col1, col2 FROM ledger WHERE id = ?";

    // roundtrip without maxBlobSize
    LedgerDef def = new LedgerDef(sizeQ, rowQ, Optional.empty());
    JSONObject jObj = LedgerDef.PARSER.toJsonObject(def);
    LedgerDef restored = LedgerDef.PARSER.toEntity(jObj);
    assertEquals(def.sizeQuery(), restored.sizeQuery());
    assertEquals(def.rowByNoQuery(), restored.rowByNoQuery());
    assertEquals(def.maxBlobSize(), restored.maxBlobSize());

    // with maxBlobSize: construct JSON manually (injectEntity omits this field)
    JSONObject jWithBlob = new JSONObject();
    jWithBlob.put(LedgerDef.Parser.SIZE_QUERY_KEY, sizeQ);
    jWithBlob.put(LedgerDef.Parser.ROW_QUERY_KEY, rowQ);
    jWithBlob.put(LedgerDef.Parser.MAX_BLOB_SIZE, 1024);
    LedgerDef defWithBlob = LedgerDef.PARSER.toEntity(jWithBlob);
    assertEquals(sizeQ, defWithBlob.sizeQuery());
    assertEquals(rowQ, defWithBlob.rowByNoQuery());
    assertEquals(Optional.of(1024), defWithBlob.maxBlobSize());
  }


  @Test
  public void testLedgerDefValidation() {
    String rowQ = "SELECT col FROM t WHERE id = ?";

    assertThrows(IllegalArgumentException.class,
        () -> new LedgerDef("  ", rowQ, Optional.empty()));
    assertThrows(IllegalArgumentException.class,
        () -> new LedgerDef("SELECT MAX(id) FROM t", "  ", Optional.empty()));
    assertThrows(IllegalArgumentException.class,
        () -> new LedgerDef("SELECT MAX(id) FROM t", rowQ, Optional.of(-1)));
  }


  @Test
  public void testSaltSeedBase64Roundtrip() {
    byte[] original = new byte[SaltSeed.LENGTH];
    new Random(42).nextBytes(original);
    SaltSeed seed = new SaltSeed(original.clone());

    JSONObject jObj = SaltSeed.PARSER.toJsonObject(seed);
    SaltSeed restored = SaltSeed.PARSER.toEntity(jObj);
    assertArrayEquals(original, restored.seed());
  }


  @Test
  public void testSaltSeedHexRoundtrip() {
    byte[] original = new byte[SaltSeed.LENGTH];
    new Random(99).nextBytes(original);
    SaltSeed seed = new SaltSeed(original.clone());

    String hexEncoded = seed.hex();
    assertEquals(SaltSeed.LENGTH * 2, hexEncoded.length());

    JSONObject jObj = new JSONObject();
    jObj.put(SaltSeed.Parser.SALT_SEED, hexEncoded);
    SaltSeed restored = SaltSeed.PARSER.toEntity(jObj);
    assertArrayEquals(original, restored.seed());
  }


  @Test
  public void testSaltSeedClear() {
    byte[] bytes = new byte[SaltSeed.LENGTH];
    new Random(7).nextBytes(bytes);
    SaltSeed seed = new SaltSeed(bytes);

    assertFalse(seed.isCleared());
    seed.clear();
    assertTrue(seed.isCleared());

    assertThrows(IllegalArgumentException.class,
        () -> SaltSeed.PARSER.injectEntity(seed, new JSONObject()));
  }


  @Test
  public void testSaltSeedWrongLength() {
    assertThrows(IllegalArgumentException.class,
        () -> new SaltSeed(new byte[31]));
  }


  @Test
  public void testDbCredentialsRoundtrip() {
    DbCredentials creds = new DbCredentials("alice", "secret123");
    JSONObject jObj = DbCredentials.PARSER.toJsonObject(creds);
    DbCredentials restored = DbCredentials.PARSER.toEntity(jObj);
    assertEquals(creds.username(), restored.username());
    assertEquals(creds.password(), restored.password());

    assertThrows(IllegalArgumentException.class,
        () -> new DbCredentials("", "pass"));
    assertThrows(IllegalArgumentException.class,
        () -> new DbCredentials("user", ""));
  }


  @Test
  public void testDbConnectionRoundtrip() {
    var parser = new DbConnection.Parser();

    // with driver class and credentials
    DbConnection con = new DbConnection(
        "jdbc:mysql://localhost:3306/mydb",
        "com.mysql.cj.jdbc.Driver",
        new DbCredentials("bob", "pass456"));
    JSONObject jObj = parser.toJsonObject(con);
    DbConnection restored = parser.toEntity(jObj);
    assertEquals(con.url(), restored.url());
    assertEquals(con.driverClass(), restored.driverClass());
    assertTrue(restored.creds().isPresent());
    assertEquals(con.creds().get().username(), restored.creds().get().username());
    assertEquals(con.creds().get().password(), restored.creds().get().password());

    // minimal: no driver class, no credentials
    DbConnection minimal = new DbConnection(
        "jdbc:h2:mem:test",
        Optional.empty(),
        Optional.empty());
    JSONObject jMin = parser.toJsonObject(minimal);
    DbConnection restoredMin = parser.toEntity(jMin);
    assertEquals(minimal.url(), restoredMin.url());
    assertTrue(restoredMin.driverClass().isEmpty());
    assertTrue(restoredMin.creds().isEmpty());

    // non-jdbc URL must be rejected
    assertThrows(IllegalArgumentException.class,
        () -> new DbConnection("mysql://localhost/db",
            Optional.empty(), Optional.empty()));
  }


  @Test
  public void testSaltSchemeParserRoundtrip() {
    // positive scheme: salts only indices 1, 3, 5
    SaltScheme scheme1 = SaltScheme.of(1, 3, 5);
    JSONObject jObj1 = SaltSchemeParser.INSTANCE.toJsonObject(scheme1);
    SaltScheme restored1 = SaltSchemeParser.INSTANCE.toEntity(jObj1);
    assertTrue(restored1.isPositive());
    assertArrayEquals(new int[]{1, 3, 5}, restored1.cellIndices());

    // negative scheme: salts all except 2 and 4
    SaltScheme scheme2 = SaltScheme.ofAllExcept(2, 4);
    JSONObject jObj2 = SaltSchemeParser.INSTANCE.toJsonObject(scheme2);
    SaltScheme restored2 = SaltSchemeParser.INSTANCE.toEntity(jObj2);
    assertFalse(restored2.isPositive());
    assertArrayEquals(new int[]{2, 4}, restored2.cellIndices());
  }

}
