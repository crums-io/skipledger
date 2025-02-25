/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.salt;


import static io.crums.util.IntegralStrings.*;
import static io.crums.sldg.salt.TableSaltTest.newSha256;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class SingleThreadShakerTest {
  
  private final byte[] seed = {
      'L', 'E', 'S', '$', ' ', '1', 'S', ' ', 'l', 'e', 's', '$' };
  
  @Test
  public void testDemo() {
    
    try (var shaker = new SingleThreadShaker(seed, newSha256())) {
      var out = System.out;
      
      final long rowNo = 1L;
      final int cell = 0;
      
      var rowSalt = shaker.rowSalt(rowNo);
      byte[] cellSalt = shaker.cellSalt(1L, cell);

      out.printf("row  (%d) salt: %s%n", rowNo, toHex(rowSalt));
      out.printf("cell (%d) salt: %s%n", cell, toHex(cellSalt));
      
      var digest = newSha256();
      
      assertEquals(
          ByteBuffer.wrap(shaker.rowSalt(rowNo, digest)),
          rowSalt);
      
      assertArrayEquals(
          TableSalt.cellSalt(rowSalt.slice(), cell, digest),
          cellSalt);
      
      // test switch
      assertNotEquals(rowSalt, shaker.rowSalt(rowNo + 1));
      assertEquals(rowSalt, shaker.rowSalt(rowNo));
      
    }
  }
  
  
  @Test
  public void testPromotedInstanceDoesNotCloseBase() {
    var baseSalter = new TableSalt(seed);
    var shaker = new SingleThreadShaker(baseSalter, newSha256());
    assertTrue(shaker.isOpen());
    shaker.close();
    assertTrue(baseSalter.isOpen());
    baseSalter.close();
  }

}







