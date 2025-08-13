/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.SourcePack.SaltSchemeR;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.Base64_32;

/**
 * 
 */
public class SourcePackTest extends SelfAwareTestCase {
  
  
  
  @Test
  public void testSaltyOne() {
    
    final Object label = new Object() {  };
    final TableSalt shaker = tableSalt(label);
    
    SaltScheme saltScheme = SaltScheme.SALT_ALL;
    
    var packBuilder = new SourcePackBuilder(saltScheme);
    
    var rowBuilder = new SourceRowBuilder(saltScheme, shaker);
    
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
    final TableSalt shaker = tableSalt(label);
    
    SaltScheme saltScheme = SaltScheme.NO_SALT;
    
    var packBuilder = new SourcePackBuilder(saltScheme);
    
    var rowBuilder = new SourceRowBuilder(saltScheme, shaker);
    
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
    SaltScheme saltScheme = SaltScheme.NO_SALT;
    
    moonshineTest(label, saltScheme);
    
  }
  
  
  @Test
  public void testSaltAll() {
    final Object label = new Object() {  };
    SaltScheme saltScheme = SaltScheme.SALT_ALL;
    
    moonshineTest(label, saltScheme);
    
  }
  
  
  @Test
  public void testSaltyFew() {
    final Object label = new Object() {  };
    
    SaltScheme scheme;
    int[] indices = new int[] { 1, 2 };
    scheme = new SaltScheme() {
      @Override
      public boolean isPositive() {
        return true;
      }
      @Override
      public int[] cellIndices() {
        return indices;
      }
    };
    moonshineTest(label, scheme);
  }
  
  
  
  @Test
  public void testNoSaltRandomLog() {
    final Object label = new Object() {  };
    int count = 1000;
    SaltScheme saltScheme = SaltScheme.NO_SALT;
    randomLogTest(label, saltScheme, count);
  }
  
  
  
  @Test
  public void testSaltAllRandomLog() {
    final Object label = new Object() {  };
    int count = 8000;
    SaltScheme saltScheme = SaltScheme.SALT_ALL;
    randomLogTest(label, saltScheme, count);
  }
  
  
  
  @Test
  public void testSaltSomeRandomLog() {
    final Object label = new Object() {  };
    int count = 3000;
    SaltScheme saltScheme = SaltSchemeR.saltOnlyInstance(new int[] { 3, 11, 12, 27 } );
    randomLogTest(label, saltScheme, count);
  }
  
  
  
  
  private void moonshineTest(Object testLabel, SaltScheme saltScheme) {
    
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
    
    doTest(testLabel, saltScheme, rowNos, values);
    
  }
  
  
  
  private void randomLogTest(Object testLabel, SaltScheme saltScheme, int count) {
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

    doTest(testLabel, saltScheme, rowNos, values);
  }
 
  
  
  private void doTest(Object testLabel, SaltScheme saltScheme, long[] rowNos, Object[][] values) {
    final TableSalt shaker = saltScheme.hasSalt() ? tableSalt(testLabel) : null;
    var rowBuilder = new SourceRowBuilder(saltScheme, shaker);
    var packBuilder = new SourcePackBuilder(saltScheme);
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
