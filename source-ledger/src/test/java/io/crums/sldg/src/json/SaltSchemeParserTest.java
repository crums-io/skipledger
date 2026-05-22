/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src.json;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.crums.sldg.src.SaltScheme;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;


/**
 * Roundtrip and error tests for {@link SaltSchemeParser}.
 */
public class SaltSchemeParserTest extends SelfAwareTestCase {

  private static final SaltSchemeParser PARSER = SaltSchemeParser.PARSER;


  // ---- Helpers -------------------------------------------------------------

  private static void assertRoundtrip(SaltScheme scheme) {
    JSONObject jObj = PARSER.toJsonObject(scheme);
    SaltScheme scheme2 = PARSER.toEntity(jObj);
    assertTrue(scheme.equals(scheme2));
  }


  // ---- Special constants ---------------------------------------------------

  @Test
  public void testNoSalt() {
    final Object label = new Object() { };
    JSONObject jObj = PARSER.toJsonObject(SaltScheme.NO_SALT);
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(jObj);

    assertEquals(1L, jObj.get(SaltSchemeParser.SALT_CODE_KEY), "salt_code=1 for positive");
    JSONArray arr = (JSONArray) jObj.get(SaltSchemeParser.CELL_INDICES_KEY);
    assertTrue(arr.isEmpty(), "empty cell_indices for NO_SALT");
    assertSame(SaltScheme.NO_SALT, PARSER.toEntity(jObj), "roundtrip returns singleton");
  }


  @Test
  public void testSaltAll() {
    final Object label = new Object() { };
    JSONObject jObj = PARSER.toJsonObject(SaltScheme.SALT_ALL);
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(jObj);

    assertEquals(0L, jObj.get(SaltSchemeParser.SALT_CODE_KEY), "salt_code=0 for negative");
    JSONArray arr = (JSONArray) jObj.get(SaltSchemeParser.CELL_INDICES_KEY);
    assertTrue(arr.isEmpty(), "empty cell_indices for SALT_ALL");
    assertSame(SaltScheme.SALT_ALL, PARSER.toEntity(jObj), "roundtrip returns singleton");
  }


  // ---- Mixed schemes -------------------------------------------------------

  @Test
  public void testPositiveMixed() {
    final Object label = new Object() { };
    SaltScheme scheme = SaltScheme.of(1, 3);   // salt only cols 1 and 3
    assertRoundtrip(scheme);

    JSONObject jObj = PARSER.toJsonObject(scheme);
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(jObj);

    assertEquals(1L, jObj.get(SaltSchemeParser.SALT_CODE_KEY));
    JSONArray arr = (JSONArray) jObj.get(SaltSchemeParser.CELL_INDICES_KEY);
    assertEquals(2, arr.size());
    assertEquals(1L, arr.get(0));
    assertEquals(3L, arr.get(1));
  }


  @Test
  public void testNegativeMixed() {
    final Object label = new Object() { };
    SaltScheme scheme = SaltScheme.ofAllExcept(0, 2);  // salt all except cols 0 and 2
    assertRoundtrip(scheme);

    JSONObject jObj = PARSER.toJsonObject(scheme);
    System.out.println("=== " + method(label) + " ===");
    JsonPrinter.println(jObj);

    assertEquals(0L, jObj.get(SaltSchemeParser.SALT_CODE_KEY));
    JSONArray arr = (JSONArray) jObj.get(SaltSchemeParser.CELL_INDICES_KEY);
    assertEquals(2, arr.size());
    assertEquals(0L, arr.get(0));
    assertEquals(2L, arr.get(1));
  }


  @Test
  public void testIndicesAreSortedOnRoundtrip() {
    // SaltScheme.of sorts its indices; verify wire order matches
    SaltScheme scheme = SaltScheme.of(5, 0, 3);
    JSONObject jObj = PARSER.toJsonObject(scheme);
    JSONArray arr = (JSONArray) jObj.get(SaltSchemeParser.CELL_INDICES_KEY);
    assertEquals(0L, arr.get(0));
    assertEquals(3L, arr.get(1));
    assertEquals(5L, arr.get(2));
    assertRoundtrip(scheme);
  }


  // ---- Error cases ---------------------------------------------------------

  @Test
  public void testInvalidSaltCode() {
    JSONObject jObj = new JSONObject();
    jObj.put(SaltSchemeParser.SALT_CODE_KEY, 2L);   // only 0 and 1 are valid
    jObj.put(SaltSchemeParser.CELL_INDICES_KEY, new JSONArray());
    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }


  @Test
  public void testMissingSaltCode() {
    JSONObject jObj = new JSONObject();
    jObj.put(SaltSchemeParser.CELL_INDICES_KEY, new JSONArray());
    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }


  @Test
  public void testMissingCellIndices() {
    JSONObject jObj = new JSONObject();
    jObj.put(SaltSchemeParser.SALT_CODE_KEY, 1L);
    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }
}
