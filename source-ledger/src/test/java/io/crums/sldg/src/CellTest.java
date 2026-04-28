/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Cell} and its inner implementations.
 */
public class CellTest {

  @Test
  public void testRedactedCell() {
    byte[] hashBytes = new byte[HASH_WIDTH];
    new Random(7L).nextBytes(hashBytes);
    ByteBuffer hashBuf = ByteBuffer.wrap(hashBytes);

    var redacted = new Cell.Redacted(hashBuf);

    assertFalse(redacted.hasData());
    assertTrue(redacted.isRedacted());
    assertFalse(redacted.hasSalt());
    assertEquals(DataType.HASH, redacted.dataType());

    assertThrows(NoSuchElementException.class, () -> redacted.data());
    assertThrows(NoSuchElementException.class, () -> redacted.value());

    assertEquals(ByteBuffer.wrap(hashBytes), redacted.hash());
  }


  @Test
  public void testRedactViaCell() {
    // BIG_INT cell
    var bigIntData = DataType.BIG_INT.toByteBuffer(BigInteger.valueOf(12345L));
    var bigIntCell = new Cell.UnsaltedReveal(DataType.BIG_INT, bigIntData);
    var bigIntHash = bigIntCell.hash();

    Cell bigIntRedacted = bigIntCell.redact();
    assertTrue(bigIntRedacted.isRedacted());
    assertEquals(bigIntHash, bigIntRedacted.hash());

    // redact() on an already-redacted cell returns the same instance
    assertSame(bigIntRedacted, bigIntRedacted.redact());

    // BIG_DEC cell
    var bigDecData = DataType.BIG_DEC.toByteBuffer(new BigDecimal("9.99"));
    var bigDecCell = new Cell.UnsaltedReveal(DataType.BIG_DEC, bigDecData);
    var bigDecHash = bigDecCell.hash();

    Cell bigDecRedacted = bigDecCell.redact();
    assertTrue(bigDecRedacted.isRedacted());
    assertEquals(bigDecHash, bigDecRedacted.hash());
    assertSame(bigDecRedacted, bigDecRedacted.redact());
  }


  @Test
  public void testCellEquality() {
    // Two UnsaltedReveal cells with same type and data are equal
    var data = DataType.BIG_INT.toByteBuffer(BigInteger.valueOf(42L));
    var cell1 = new Cell.UnsaltedReveal(DataType.BIG_INT, data);
    var cell2 = new Cell.UnsaltedReveal(DataType.BIG_INT, data.slice());
    assertEquals(cell1, cell2);

    // Same raw bytes but different types → different type byte in hash → not equal
    var rawBytes = ByteBuffer.wrap(new byte[] { 65, 66, 67 });
    var bigIntCell = new Cell.UnsaltedReveal(DataType.BIG_INT, rawBytes.slice());
    var stringCell = new Cell.UnsaltedReveal(DataType.STRING, rawBytes.slice());
    assertNotEquals(bigIntCell, stringCell);

    // Revealed cell and its redacted counterpart differ (dataSize differs: actual vs -1)
    var original  = new Cell.UnsaltedReveal(DataType.BIG_INT, data.slice());
    Cell redacted = original.redact();
    assertNotEquals(original, redacted);
  }


  @Test
  public void testUnsaltedNullCell() {
    Cell nullCell = Cell.UNSALTED_NULL;

    assertEquals(DataType.NULL, nullCell.dataType());
    assertTrue(nullCell.hasData());
    assertFalse(nullCell.hasSalt());

    ByteBuffer data = nullCell.data();
    assertEquals(DataType.NULL.size(), data.remaining()); // 5 bytes
    while (data.hasRemaining())
      assertEquals((byte) 0, data.get());
  }

}
