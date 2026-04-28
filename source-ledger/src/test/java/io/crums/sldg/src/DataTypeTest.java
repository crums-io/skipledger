/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static org.junit.jupiter.api.Assertions.*;
import static io.crums.sldg.src.SharedConstants.HASH_WIDTH;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class DataTypeTest {
  
  /** Honestly, this is a test of my understanding of enums. */
  @Test
  public void testForOrdinal() {
    for (var type : DataType.values())
      assertEquals(type, DataType.forOrdinal(type.ordinal()));
  }



  private void assertRoundtrip(DataType type, Object value) {
    ByteBuffer bytes = type.toByteBuffer(value);
    if (value == null)
      assertNull(DataType.NULL.toValue(bytes));
    else
      assertEquals(value, type.toValue(bytes));
  }

  private void assertIllegalArg(DataType type, Object value) {
    assertIllegalArg(type, value, true);
  }

  private void assertIllegalArg(DataType type, Object value, boolean print) {
    try {
      type.toByteBuffer(value);
      fail();
    } catch (IllegalArgumentException expected) {
      if (print) {
        System.out.println("[EXPECTED] " + expected);
      }
    }
  }


  @Test
  public void testString() {
    assertRoundtrip(DataType.STRING, "hello world");
    assertRoundtrip(DataType.STRING, "");
    assertRoundtrip(DataType.STRING, null);

    assertIllegalArg(DataType.STRING, 0);
  }

  @Test
  public void testLong() {
    assertRoundtrip(DataType.LONG, 23L);
    assertRoundtrip(DataType.LONG, -23L);
    assertRoundtrip(DataType.LONG, null);

    var expectedBuffer = DataType.LONG.toByteBuffer(27L);
    assertEquals(expectedBuffer, DataType.LONG.toByteBuffer(27));
    assertEquals(expectedBuffer, DataType.LONG.toByteBuffer((short) 27));
    assertEquals(expectedBuffer, DataType.LONG.toByteBuffer((byte) 27));

    assertIllegalArg(DataType.LONG, 23.0);
  }

  @Test
  public void testBigInt() {
    BigInteger value = BigInteger.valueOf(23L);

    assertRoundtrip(DataType.BIG_INT, value);
    assertRoundtrip(DataType.BIG_INT, value.negate());

    var expected = ByteBuffer.wrap(value.toByteArray());
    assertEquals(expected, DataType.BIG_INT.toByteBuffer(23L));
    assertEquals(expected, DataType.BIG_INT.toByteBuffer(23));
    assertEquals(expected, DataType.BIG_INT.toByteBuffer((short) 23));
    assertEquals(expected, DataType.BIG_INT.toByteBuffer((byte) 23));
    assertIllegalArg(DataType.BIG_INT, 27.0f);
  }

  @Test
  public void testBigDec() {
    BigInteger unscaled = BigInteger.valueOf(1234567L);
    BigDecimal value = new BigDecimal(unscaled, 2);

    assertRoundtrip(DataType.BIG_DEC, value);
    assertRoundtrip(DataType.BIG_DEC, value.negate());
    assertRoundtrip(DataType.BIG_DEC, new BigDecimal(unscaled, 127));
    assertRoundtrip(DataType.BIG_DEC, new BigDecimal(unscaled, -128));

    final long baseValue = 123L;

    var expected = DataType.BIG_DEC.toByteBuffer(BigDecimal.valueOf(baseValue));
    assertEquals(expected, DataType.BIG_DEC.toByteBuffer((int) baseValue));
    assertEquals(expected, DataType.BIG_DEC.toByteBuffer((short) baseValue));
    assertEquals(expected, DataType.BIG_DEC.toByteBuffer((byte) baseValue));

    assertIllegalArg(DataType.BIG_DEC, 27.0f);
    assertIllegalArg(DataType.BIG_DEC, new BigDecimal(unscaled, -129));
    assertIllegalArg(DataType.BIG_DEC, new BigDecimal(unscaled, 128));
  }

  @Test
  public void testDate() {
    long utc = 1_000_000_000L;
    assertRoundtrip(DataType.DATE, utc);

    var expectedBuffer = DataType.DATE.toByteBuffer(utc);
    assertEquals(expectedBuffer, DataType.DATE.toByteBuffer(new Date(utc)));
  }

  @Test
  public void testBool() {
    assertRoundtrip(DataType.BOOL, false);
    assertRoundtrip(DataType.BOOL, true);
    assertIllegalArg(DataType.BOOL, 0);
  }



  @Test
  public void testBytes() {
    byte[] bytes = new byte[77];
    {
      Random rand = new Random(56L);
      rand.nextBytes(bytes);
    }
    var buffer = ByteBuffer.wrap(bytes);

    assertRoundtrip(DataType.BYTES, buffer);
    assertRoundtrip(DataType.BYTES, ByteBuffer.wrap(new byte[0]));

    assertEquals(buffer, DataType.BYTES.toByteBuffer(bytes));
    assertIllegalArg(DataType.BYTES, (byte) 1);
  }

  
  @Test
  public void testHash() {
    byte[] bytes = new byte[HASH_WIDTH];
    {
      Random rand = new Random(11L);
      rand.nextBytes(bytes);
    }
    var buffer = ByteBuffer.wrap(bytes);

    assertRoundtrip(DataType.HASH, buffer);

    assertEquals(buffer, DataType.HASH.toByteBuffer(bytes));
    assertIllegalArg(DataType.HASH, new byte[0]);
    assertIllegalArg(DataType.HASH, new byte[HASH_WIDTH - 1]);
    assertIllegalArg(DataType.HASH, new byte[HASH_WIDTH + 1]);


  }


  @Test
  public void testNull() {
    var serial = DataType.NULL.toByteBuffer(null);
    assertNotNull(serial);
    assertEquals(DataType.NULL.size(), serial.remaining());

    assertRoundtrip(DataType.NULL, null);
    assertIllegalArg(DataType.NULL, "");

    
    ByteBuffer bad;
    {
      byte[] array = new byte[DataType.NULL.size()];
      array[1] = 1;
      bad = ByteBuffer.wrap(array);
    }
    assertBadNull(bad);
    bad = ByteBuffer.allocate(DataType.NULL.size() - 1);
    assertBadNull(bad);
  }

  private void assertBadNull(ByteBuffer bad) {
    try {
      DataType.NULL.toValue(bad);
      fail();
    } catch (IllegalArgumentException expected) {
      System.out.println("[EXPECTED] " + expected);
    }
  }



}





