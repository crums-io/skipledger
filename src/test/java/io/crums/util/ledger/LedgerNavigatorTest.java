/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

/**
 * 
 */
public class LedgerNavigatorTest extends SelfAwareTestCase {
  
  
  @Test
  public void testEmpty() {
    SkipLedger ledger = new VolatileLedger(0);
    LedgerNavigator nav = new LedgerNavigator(ledger);
    assertEquals(0, nav.size());
    
    Random random = new Random(-9);
    byte[] mockEntry = new byte[nav.hashWidth()];
    random.nextBytes(mockEntry);
    
    try {
      
      nav.addEntry(ByteBuffer.wrap(mockEntry));
      fail();
    } catch (RuntimeException expected) {  }
  }
  
  
  @Test
  public void testSkipPath() {
    
    Object label = new Object() { };
    
    
    final int rows = 1024 * 1024 + 63;
    System.out.println("== " + method(label) + "");
    System.out.print("Generating ledger with " + rows + " random entries");

    SkipLedger ledger = new VolatileLedger(rows);
    LedgerNavigator nav = new LedgerNavigator(ledger);
    

    Random random = new Random(9);
    byte[] mockHash = new byte[nav.hashWidth()];
    ByteBuffer mockHashBuffer = ByteBuffer.wrap(mockHash);
    
    for (int count = rows; count-- > 0; ) {
      random.nextBytes(mockHash);
      mockHashBuffer.clear();
      nav.addEntry(mockHashBuffer);
    }
    
    assertEquals(rows, nav.size());
    

    System.out.println();
    System.out.println("skip path 7 -> 625:");
    print(nav.skipPath(7, 625));
    System.out.println("V-form:");
    print(nav.skipPath());
    System.out.println("== " + method(label) + ": [DONE]");
  }
  
  
  private void print(LinkedPath skipPath) {
    System.out.println();
    
    for (Row row : skipPath.path())
      System.out.println(row);

    System.out.println();
  }

}
