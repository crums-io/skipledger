/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src.json;


import static io.crums.sldg.src.SharedConstants.DIGEST;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.salt.EpochedTableSalt.EpochSeed;
import io.crums.sldg.salt.TableSalt;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;


/**
 * Roundtrip and error tests for {@link EpochSeedParser} and {@link TableSaltReader}.
 */
public class EpochSeedParserTest extends SelfAwareTestCase {

  private static final EpochSeedParser PARSER = EpochSeedParser.PARSER;
  private static final TableSaltReader READER  = TableSaltReader.READER;


  // ---- Helpers -------------------------------------------------------------

  /** Derives a deterministic 32-byte seed from the test method name. */
  private byte[] seedFor(Object label) {
    return DIGEST.newDigest().digest(method(label).getBytes());
  }

  private static void assertSeedRoundtrip(EpochSeed seed) {
    JSONObject jObj = PARSER.toJsonObject(seed);
    EpochSeed seed2 = PARSER.toEntity(jObj);
    assertEquals(seed.startRow(), seed2.startRow());
    assertArrayEquals(seed.seed(), seed2.seed());
  }


  // ---- EpochSeedParser roundtrips ------------------------------------------

  @Test
  public void testSingleSeed() {
    final Object label = new Object() { };
    EpochSeed seed = new EpochSeed(1L, seedFor(label));
    assertSeedRoundtrip(seed);

    JSONObject jObj = PARSER.toJsonObject(seed);
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(jObj);

    assertEquals(1L, jObj.get(EpochSeedParser.SEED_START_KEY));
    String encoded = (String) jObj.get(EpochSeedParser.SEED_KEY);
    assertNotNull(encoded);
    assertEquals(43, encoded.length(), "Base64_32 is always 43 chars");
  }


  @Test
  public void testHighStartRow() {
    final Object label = new Object() { };
    EpochSeed seed = new EpochSeed(100_000L, seedFor(label));
    assertSeedRoundtrip(seed);

    JSONObject jObj = PARSER.toJsonObject(seed);
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(jObj);

    assertEquals(100_000L, jObj.get(EpochSeedParser.SEED_START_KEY));
  }


  // ---- EpochSeedParser error cases -----------------------------------------

  @Test
  public void testWrongSeedLength() {
    JSONObject jObj = new JSONObject();
    jObj.put(EpochSeedParser.SEED_START_KEY, 1L);
    jObj.put(EpochSeedParser.SEED_KEY, "tooshort");   // not 43 chars
    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }


  @Test
  public void testHexSeedRejected() {
    // 64-char hex is NOT accepted — only Base64_32 (43 chars) is supported
    JSONObject jObj = new JSONObject();
    jObj.put(EpochSeedParser.SEED_START_KEY, 1L);
    jObj.put(EpochSeedParser.SEED_KEY, "a".repeat(64));
    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }


  @Test
  public void testMissingSeed() {
    JSONObject jObj = new JSONObject();
    jObj.put(EpochSeedParser.SEED_START_KEY, 1L);
    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }


  @Test
  public void testMissingSeedStart() {
    EpochSeed seed = new EpochSeed(1L, seedFor(new Object() { }));
    JSONObject jObj = PARSER.toJsonObject(seed);
    jObj.remove(EpochSeedParser.SEED_START_KEY);
    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }


  // ---- TableSaltReader -----------------------------------------------------

  @Test
  public void testTableSaltSingleEpoch() {
    final Object label = new Object() { };
    EpochSeed seed = new EpochSeed(1L, seedFor(label));

    JSONArray arr = new JSONArray();
    arr.add(PARSER.toJsonObject(seed));
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(arr);

    TableSalt salt = READER.toTableSalt(arr);
    assertNotNull(salt);
    // Verify the salt produces stable hashes (at least doesn't throw)
    byte[] h = salt.rowSalt(1L, DIGEST.newDigest());
    assertNotNull(h);
    assertEquals(32, h.length);
  }


  @Test
  public void testTableSaltMultipleEpochs() {
    final Object label = new Object() { };
    byte[] seed1 = seedFor(label);
    byte[] seed2 = DIGEST.newDigest().digest(seed1);   // derive second seed

    List<EpochSeed> seeds = List.of(
        new EpochSeed(  1L, seed1),
        new EpochSeed(100L, seed2));

    JSONArray arr = new JSONArray();
    seeds.forEach(s -> arr.add(PARSER.toJsonObject(s)));
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(arr);

    TableSalt salt = READER.toTableSalt(arr);
    assertNotNull(salt);

    // Rows in epoch 1 and epoch 2 should hash differently
    byte[] h1 = salt.rowSalt(  1L, DIGEST.newDigest());
    byte[] h2 = salt.rowSalt(100L, DIGEST.newDigest());
    assertFalse(Arrays.equals(h1, h2), "different epochs produce different hashes");
  }


  @Test
  public void testToEntityDelegatesToParser() {
    // TableSaltReader.toEntity(JSONObject) is the single-epoch path
    final Object label = new Object() { };
    EpochSeed seed = new EpochSeed(1L, seedFor(label));
    JSONObject jObj = PARSER.toJsonObject(seed);
    TableSalt salt = READER.toEntity(jObj);
    assertNotNull(salt);
  }


  @Test
  public void testEmptyArrayThrows() {
    assertThrows(JsonParsingException.class,
        () -> READER.toTableSalt(new JSONArray()));
  }
}
