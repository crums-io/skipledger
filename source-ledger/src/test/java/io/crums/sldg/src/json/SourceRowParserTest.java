/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src.json;


import static io.crums.sldg.src.SharedConstants.DIGEST;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import org.junit.jupiter.api.Test;

import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.DataType;
import io.crums.sldg.src.SaltScheme;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.SourceRowBuilder;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;


/**
 * Roundtrip and error tests for {@link SourceRowParser}.
 */
public class SourceRowParserTest extends SelfAwareTestCase {

  private static final SourceRowParser PARSER = SourceRowParser.INSTANCE;


  // ---- Helpers -----------------------------------------------------------

  private TableSalt testShaker(Object label) {
    byte[] salt = DIGEST.newDigest().digest(method(label).getBytes());
    return new TableSalt(salt);
  }

  /** Roundtrip: row → JSON → row2; assert equal hashes. */
  private static void assertRoundtrip(SourceRow row) {
    JSONObject jObj = PARSER.toJsonObject(row);
    SourceRow row2 = PARSER.toEntity(jObj);
    assertEquals(row.no(), row2.no());
    assertEquals(row.cells().size(), row2.cells().size());
    assertEquals(row.hash(), row2.hash());
  }


  // ---- Unsalted rows -----------------------------------------------------

  @Test
  public void testUnsaltedInferrableTypes() {
    // null, boolean, long, string — written as bare JSON values
    var builder = new SourceRowBuilder();
    SourceRow row = builder.buildRow(1L, (Object) null, true, 42L, "hello");
    assertRoundtrip(row);

    JSONObject jObj = PARSER.toJsonObject(row);
    JSONArray cells = (JSONArray) jObj.get(SourceRowParser.CELLS_KEY);
    assertNull(cells.get(0),              "null → bare null");
    assertEquals(Boolean.TRUE, cells.get(1), "bool → bare boolean");
    assertEquals(42L, cells.get(2),       "long → bare long");
    assertEquals("hello", cells.get(3),   "string → bare string");
    assertNull(jObj.get(SourceRowParser.SALT_KEY), "no row salt key");
  }


  @Test
  public void testUnsaltedNonInferrableTypes() {
    // DATE, BIG_DEC, BIG_INT, BYTES, HASH — require object form with "type" key
    var builder = new SourceRowBuilder();
    long utcMillis = new Date(2025, 0, 15).getTime(); // Jan 2025
    BigDecimal dec = new BigDecimal("99.9500");
    BigInteger bigInt = new BigInteger("123456789012345678901234567890");
    byte[] rawBytes = { 1, 2, 3, 4, 5 };
    byte[] hashBytes = new byte[32];
    for (int i = 0; i < 32; i++) hashBytes[i] = (byte) i;

    SourceRow row = builder.buildRow(
        2L,
        java.util.List.of(
            DataType.DATE, DataType.BIG_DEC, DataType.BIG_INT,
            DataType.BYTES, DataType.HASH),
        java.util.List.of(
            utcMillis, dec, bigInt,
            ByteBuffer.wrap(rawBytes), ByteBuffer.wrap(hashBytes)));

    assertRoundtrip(row);

    // Verify BIG_DEC is written as a string
    JSONObject jObj = PARSER.toJsonObject(row);
    JSONArray cells = (JSONArray) jObj.get(SourceRowParser.CELLS_KEY);
    JSONObject decCell = (JSONObject) cells.get(1);
    assertEquals(SourceRowParser.T_DEC, decCell.get(SourceRowParser.TYPE_KEY));
    assertEquals("99.9500", decCell.get(SourceRowParser.VALUE_KEY));

    // Verify BIG_INT is written as a string
    JSONObject bigIntCell = (JSONObject) cells.get(2);
    assertEquals(SourceRowParser.T_BIGINT, bigIntCell.get(SourceRowParser.TYPE_KEY));
    assertEquals("123456789012345678901234567890", bigIntCell.get(SourceRowParser.VALUE_KEY));
  }


  @Test
  public void testUnsaltedNullOnlyRow() {
    var builder = new SourceRowBuilder();
    SourceRow row = builder.buildRow(3L, (Object) null);
    assertRoundtrip(row);
  }


  // ---- Row-salted rows ---------------------------------------------------

  @Test
  public void testRowSaltedMixedTypes() {
    Object label = new Object() { };
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, testShaker(label));
    SourceRow row = builder.buildRow(10L, (Object) null, true, 99L, "world");
    assertTrue(row.rowSalt().isPresent());
    assertRoundtrip(row);

    // Row salt appears at row level, not cell level
    JSONObject jObj = PARSER.toJsonObject(row);
    assertNotNull(jObj.get(SourceRowParser.SALT_KEY), "row salt present");
    JSONArray cells = (JSONArray) jObj.get(SourceRowParser.CELLS_KEY);
    // Cells in compact form (no cell-level salt keys)
    assertNull(cells.get(0));
    assertEquals(true, cells.get(1));
    assertEquals(99L, cells.get(2));
    assertEquals("world", cells.get(3));
  }


  @Test
  public void testRowSaltedNullAndDate() {
    Object label = new Object() { };
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, testShaker(label));
    long utc = System.currentTimeMillis();
    SourceRow row = builder.buildRow(
        11L,
        java.util.List.of(DataType.NULL, DataType.DATE),
        Arrays.asList(null, utc));
    assertRoundtrip(row);
  }


  // ---- Individually salted cells -----------------------------------------

  @Test
  public void testIndividuallySaltedCells() {
    // Build a SALT_ALL row, then redact cell 0.
    // Redaction hides the row-level salt, so the surviving revealed cell
    // must carry its own salt in JSON — that is the "individually salted cell" case.
    Object label = new Object() { };
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, testShaker(label));
    SourceRow full = builder.buildRow(20L, "redact me", "salted");
    SourceRow row = full.redact(0);

    assertTrue(row.hasRedactions());
    assertTrue(row.cells().get(1).hasSalt(), "revealed cell has salt");
    assertTrue(row.rowSalt().isEmpty(), "row salt hidden after redaction");

    assertRoundtrip(row);

    // Verify the surviving revealed cell is written with a "salt" key
    JSONObject jObj = PARSER.toJsonObject(row);
    assertNull(jObj.get(SourceRowParser.SALT_KEY), "no row-level salt");
    JSONArray cells = (JSONArray) jObj.get(SourceRowParser.CELLS_KEY);
    // Cell 0 is redacted
    JSONObject redactedCell = (JSONObject) cells.get(0);
    assertEquals(SourceRowParser.T_REDACT, redactedCell.get(SourceRowParser.TYPE_KEY));
    // Cell 1 is revealed and individually salted
    JSONObject saltedCell = (JSONObject) cells.get(1);
    assertNotNull(saltedCell.get(SourceRowParser.SALT_KEY), "column 1 has salt key");
    assertEquals("salted", saltedCell.get(SourceRowParser.VALUE_KEY));
  }


  // ---- Redacted cells ----------------------------------------------------

  @Test
  public void testRedactedCell() {
    Object label = new Object() { };
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, testShaker(label));
    SourceRow full = builder.buildRow(30L, "keep", "redact me");
    SourceRow redacted = full.redact(1);

    assertTrue(redacted.hasRedactions());
    assertTrue(redacted.rowSalt().isEmpty(), "row salt hidden after redaction");

    assertRoundtrip(redacted);

    // Verify redacted cell uses { "type": "X", "hash": "..." }
    JSONObject jObj = PARSER.toJsonObject(redacted);
    JSONArray cells = (JSONArray) jObj.get(SourceRowParser.CELLS_KEY);
    JSONObject xCell = (JSONObject) cells.get(1);
    assertEquals(SourceRowParser.T_REDACT, xCell.get(SourceRowParser.TYPE_KEY));
    assertNotNull(xCell.get(SourceRowParser.HASH_KEY));
    assertNull(xCell.get(SourceRowParser.VALUE_KEY));
  }


  // ---- Compact bare value parsing ----------------------------------------

  @Test
  public void testReadBareValues() {
    // Construct JSON manually with bare cell values
    JSONObject jObj = new JSONObject();
    jObj.put(SourceRowParser.ROW_KEY, 99L);
    JSONArray cells = new JSONArray();
    cells.add(null);
    cells.add(Boolean.FALSE);
    cells.add(7L);
    cells.add("bare string");
    jObj.put(SourceRowParser.CELLS_KEY, cells);

    SourceRow row = PARSER.toEntity(jObj);
    assertEquals(99L, row.no());
    assertEquals(DataType.NULL,   row.cellTypes().get(0));
    assertEquals(DataType.BOOL,   row.cellTypes().get(1));
    assertEquals(DataType.LONG,   row.cellTypes().get(2));
    assertEquals(DataType.STRING, row.cellTypes().get(3));
    assertEquals(Boolean.FALSE, row.cells().get(1).value());
    assertEquals(7L,            row.cells().get(2).value());
    assertEquals("bare string", row.cells().get(3).value());
  }


  // ---- Error cases -------------------------------------------------------

  @Test
  public void testBareDoubleRejected() {
    JSONObject jObj = new JSONObject();
    jObj.put(SourceRowParser.ROW_KEY, 1L);
    JSONArray cells = new JSONArray();
    cells.add(3.14);   // Double — not supported as bare value
    jObj.put(SourceRowParser.CELLS_KEY, cells);

    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }


  @Test
  public void testRedactedCellWrongHashLength() {
    JSONObject jObj = new JSONObject();
    jObj.put(SourceRowParser.ROW_KEY, 1L);
    JSONArray cells = new JSONArray();
    JSONObject bad = new JSONObject();
    bad.put(SourceRowParser.TYPE_KEY, SourceRowParser.T_REDACT);
    bad.put(SourceRowParser.HASH_KEY, "tooshort");  // not 43 or 64 chars
    cells.add(bad);
    jObj.put(SourceRowParser.CELLS_KEY, cells);

    assertThrows(JsonParsingException.class, () -> PARSER.toEntity(jObj));
  }

}
