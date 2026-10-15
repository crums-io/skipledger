/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.sldg.salt.TableSalt;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.api.Base64_32;

/**
 *
 */
public class SourcePackTest extends SelfAwareTestCase {



  @Test
  public void testSaltyOne() {

    final Object label = new Object() {  };
    final TableSalt shaker = tableSalt(label);

    var packBuilder = new SourcePackBuilder(true);

    var rowBuilder = new SourceRowBuilder(shaker);

    final long no = 11L;

    var row = rowBuilder.buildRow(no, "hello", "row");

    assertTrue( packBuilder.add(row) );

    SourcePack pack = packBuilder.build();
    assertEquals(List.of(no), pack.sourceNos());

    assertEquals(row, pack.findSourceByNo(no).get());

    assertSerialRoundtrip(pack);
  }


  @Test
  public void testOneUnsalted() {

    final Object label = new Object() {  };

    var packBuilder = new SourcePackBuilder(false);

    var rowBuilder = new SourceRowBuilder();

    final long no = 11L;

    var row = rowBuilder.buildRow(no, "hello", "row");

    assertTrue( packBuilder.add(row) );

    SourcePack pack = packBuilder.build();
    assertEquals(List.of(no), pack.sourceNos());

    assertEquals(row, pack.findSourceByNo(no).get());

    assertSerialRoundtrip(pack);
  }


  @Test
  public void testNoSalt() {
    final Object label = new Object() {  };
    moonshineTest(label, false);
  }


  @Test
  public void testSaltAll() {
    final Object label = new Object() {  };
    moonshineTest(label, true);
  }



  @Test
  public void testNoSaltRandomLog() {
    final Object label = new Object() {  };
    int count = 1000;
    randomLogTest(label, false, count);
  }



  @Test
  public void testSaltAllRandomLog() {
    final Object label = new Object() {  };
    int count = 8000;
    randomLogTest(label, true, count);
  }




  @Test
  public void testBigNumericNoSalt() {
    final Object label = new Object() {  };
    bigNumericTest(label, false);
  }


  @Test
  public void testBigNumericSaltAll() {
    final Object label = new Object() {  };
    bigNumericTest(label, true);
  }


  /**
   * Verifies that BIG_INT and BIG_DEC cell values survive a serialize/deserialize
   * roundtrip with explicit value assertions (not just equality of row objects).
   */
  @Test
  public void testBigNumericCellValues() {
    final Object label = new Object() {  };
    final TableSalt shaker = tableSalt(label);

    BigInteger bigInt = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    BigDecimal bigDec = new BigDecimal("12345.678");

    var rowBuilder  = new SourceRowBuilder(shaker);
    var packBuilder = new SourcePackBuilder(true);

    final long no = 5L;
    packBuilder.add(rowBuilder.buildRow(no, bigInt, bigDec));

    SourcePack pack = packBuilder.build();

    // verify values in the in-memory pack
    var row = pack.findSourceByNo(no).get();
    assertEquals(bigInt, row.cells().get(0).value());
    assertEquals(bigDec, row.cells().get(1).value());

    // verify values survive serialization
    SourcePack packCopy = SourcePack.load(pack.serialize());
    var rowCopy = packCopy.findSourceByNo(no).get();
    assertEquals(bigInt, rowCopy.cells().get(0).value());
    assertEquals(bigDec, rowCopy.cells().get(1).value());
  }


  private void bigNumericTest(Object testLabel, boolean salted) {
    long[] rowNos = { 1L, 7L, 42L, 100L };

    BigInteger beyondMax  = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    BigInteger beyondMin  = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);

    Object[][] values = {
        { BigInteger.ZERO,  BigDecimal.ZERO },
        { beyondMax,        new BigDecimal("99999.999") },
        { beyondMin,        new BigDecimal("-0.001"),  "label" },
        { 42L, BigInteger.valueOf(1_234_567_890L), new BigDecimal("3.14159"), "pi" },
    };

    doTest(testLabel, salted, rowNos, values);
  }


  private void moonshineTest(Object testLabel, boolean salted) {

    long[] rowNos = {
        3L,
        55L,
        56L,
        89L,
        90L,
        91L,
        108L
    };

    final long utc = 10_000_000;

    Object[][] values = {
        { "Dear", "Mr.", "Moonshine," },
        {  "CAT", 5},
        {  "apogee", 345002, "units", "millis"},
        {  new Date(utc), "EZ-f0045"},
        {  "ref:", "d77-", 610649, true, "ok"},
        {  "STATUS:", null},
        {  "sig:", random(99L, 68)},
    };

    doTest(testLabel, salted, rowNos, values);

  }



  private void randomLogTest(Object testLabel, boolean salted, int count) {
    Random rand = new Random(method(testLabel).hashCode());
    // exclusive limits..
    final int MAX_INCR = 1000;
    final int MAX_CC = 128;
    final int MAX_TOKEN_LEN = 37;

    long prevNo = 0L;
    long[] rowNos = new long[count];
    Object[][] values = new Object[count][];
    byte[] b32 = new byte[32];

    for (int r = 0; r < count; ++r) {

      rowNos[r] = prevNo + 1L + rand.nextInt(MAX_INCR);
      prevNo = rowNos[r];
      final int cc = 1 + rand.nextInt(MAX_CC);
      values[r] = new Object[cc];

      for (int c = 0; c < cc; ++c) {
        int len = 1 + rand.nextInt(MAX_TOKEN_LEN);
        rand.nextBytes(b32);
        values[r][c] = Base64_32.encode(b32).substring(0, len);
      }
    }

    doTest(testLabel, salted, rowNos, values);
  }



  private void doTest(Object testLabel, boolean salted, long[] rowNos, Object[][] values) {
    final TableSalt shaker = salted ? tableSalt(testLabel) : null;
    var rowBuilder = new SourceRowBuilder(shaker);
    var packBuilder = new SourcePackBuilder(salted);
    assert rowNos.length == values.length;
    var expectedRows = new ArrayList<SourceRow>();
    for (int index = 0; index < rowNos.length; ++index) {
      var row = rowBuilder.buildRow(rowNos[index], values[index]);
      assertTrue( packBuilder.add(row) );
      expectedRows.add(row);
    }
    var pack = packBuilder.build();
    assertEquals(expectedRows, pack.sources());
    assertSerialRoundtrip(pack);
  }





  private void assertSerialRoundtrip(SourcePack pack) {
    var memBuffer = pack.serialize();
    SourcePack packCopy = SourcePack.load(memBuffer);
    assertFalse(memBuffer.hasRemaining());
    assertEquals(pack.sources(), packCopy.sources());
  }


  private TableSalt tableSalt(Object label) {
    return new TableSalt( seed( method(label) ) );
  }

  static byte[] seed(String seed) {
    return DIGEST.newDigest().digest(seed.getBytes());
  }

  static byte[] random(long seed, int size) {
    byte[] out = new byte[size];
    new Random(seed).nextBytes(out);
    return out;
  }

}
