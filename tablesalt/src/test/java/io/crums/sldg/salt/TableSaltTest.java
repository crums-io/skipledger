/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.salt;


import static io.crums.util.IntegralStrings.*;
import static org.junit.jupiter.api.Assertions.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.junit.jupiter.api.Test;


/**
 * 
 */
public class TableSaltTest {

  private byte[] seed = { 'h', 'e', 'l', 'l', '0', ' ', 'w', 'o', 'd', 'l'};
  

  @Test
  public void testOne() {
    try (var shaker = new TableSalt(seed)) {
      
      var digest = newSha256();
      
      byte[] rowSalt = shaker.rowSalt(1L, digest);
      byte[] cellSalt = TableSalt.cellSalt(rowSalt, 1L, digest);
      
      var out = System.out;
      
      out.println("row (1) salt: " + toHex(rowSalt));
      out.println("cell (1) salt: " + toHex(cellSalt));
    }
  }
  
  
  @Test
  public void testConstructorSeedArgIsCleared() {
    var shaker = new TableSalt(seed);
    for (int index = seed.length; index-- > 0; )
      assertEquals(0, seed[index]);
    shaker.close();
  }
  
  
  @Test
  public void testClose() {
    var shaker = new TableSalt(seed);

    assertTrue(shaker.isOpen());
    shaker.close();
    assertFalse(shaker.isOpen());
    
    try {
      shaker.rowSalt(5L, newSha256());
      fail();
    } catch (IllegalStateException expected) {   }
  }
  
  
  
  @Test
  public void testDigestArgIsReset() {
    var shaker = new TableSalt(seed);
    var digest = newSha256();
    
    final long rowNo = 7L;
    final int cellIndex = 57;

    byte[] rowSalt = shaker.rowSalt(rowNo, digest);
    {
      // verify digest is reset
      byte[] rs2 = shaker.rowSalt(rowNo, digest);
      assertArrayEquals(rowSalt, rs2);
    }
    
    byte[] cellSalt = TableSalt.cellSalt(rowSalt, cellIndex, digest);
    {
      // verify digest is reset
      byte[] cs2 = TableSalt.cellSalt(rowSalt, cellIndex, digest);
      assertArrayEquals(cellSalt, cs2);
    }
    shaker.close();
  }
  
  
  
  public static MessageDigest newSha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException nosx) {
      throw new RuntimeException(nosx);
    }
  }
  
}









