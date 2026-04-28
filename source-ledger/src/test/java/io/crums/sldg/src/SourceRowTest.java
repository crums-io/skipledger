/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SourceRow} and related behavior.
 */
public class SourceRowTest {

  @Test
  public void testNullRow() {
    var row = SourceRow.nullRow(1L);
    assertEquals(1L, row.no());
    assertTrue(row.cells().isEmpty());
    assertEquals(DIGEST.sentinelHash(), row.hash());

    assertThrows(IllegalArgumentException.class, () -> SourceRow.nullRow(0L));
  }


  @Test
  public void testSingleCellRowHash() {
    var builder = new SourceRowBuilder();
    var row = builder.buildRow(1L, BigInteger.valueOf(42L));

    // single-cell row: row hash == cell hash (not re-hashed)
    assertEquals(row.cells().get(0).hash(), row.hash());
  }


  @Test
  public void testMultiCellRowHash() {
    var builder = new SourceRowBuilder();
    var row = builder.buildRow(1L, 42L, BigInteger.valueOf(100L), new BigDecimal("1.5"));

    assertEquals(3, row.cells().size());

    // manually compute SHA-256( cellHash0 || cellHash1 || cellHash2 )
    var accumulator = DIGEST.newDigest();
    for (var cell : row.cells())
      accumulator.update(cell.hash());
    var expected = ByteBuffer.wrap(accumulator.digest());

    assertEquals(expected, row.hash());
  }


  @Test
  public void testRedactCell() {
    var builder = new SourceRowBuilder();
    var row = builder.buildRow(1L, 42L, BigInteger.valueOf(100L), new BigDecimal("1.5"));

    assertFalse(row.hasRedactions());

    var redacted = row.redact(1);

    assertTrue(redacted.hasRedactions());
    assertTrue(redacted.cells().get(0).hasData());   // cell 0 revealed
    assertTrue(redacted.cells().get(1).isRedacted()); // cell 1 redacted
    assertTrue(redacted.cells().get(2).hasData());   // cell 2 revealed

    // hash must be unchanged
    assertEquals(row.hash(), redacted.hash());
  }


  @Test
  public void testToStringBigTypes() {
    var builder = new SourceRowBuilder();
    var row = builder.buildRow(1L, BigInteger.valueOf(42L), new BigDecimal("1.5"));

    String s = row.toString();
    assertTrue(s.contains("G"), "BIG_INT should be prefixed with 'G': " + s);
    assertTrue(s.contains("D"), "BIG_DEC should be prefixed with 'D': " + s);
  }

}
