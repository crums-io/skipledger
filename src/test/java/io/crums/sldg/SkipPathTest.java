/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.sldg.db.VolatileTable;

/**
 * 
 */
public class SkipPathTest extends SelfAwareTestCase {
  
  

  
  
  @Test
  public void testSkipPath() {
    
    Object label = new Object() { };
    
    
    final int rows = 1024 * 1024 + 63;
    System.out.println("== " + method(label) + ": ");
    System.out.print("Generating ledger with " + rows + " random entries");
    
    Ledger ledger = new CompactLedger(new VolatileTable());

    

    Random random = new Random(9);
    byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
    ByteBuffer mockHashBuffer = ByteBuffer.wrap(mockHash);
    
    for (int count = rows; count-- > 0; ) {
      random.nextBytes(mockHash);
      mockHashBuffer.clear();
      ledger.appendRows(mockHashBuffer);
    }
    
    assertEquals(rows, ledger.size());

    System.out.println();
    System.out.println("skip path 7 -> 625:");
    print(ledger.skipPath(7, 625));
    System.out.println("V-form:");
    print(ledger.statePath());
    System.out.println("== " + method(label) + ": [DONE]");
    
    ledger.close();
  }
  
  
  private void print(Path skipPath) {
    System.out.println();
    
    for (Row row : skipPath.path())
      System.out.println(row);

    System.out.println();
  }
  
  

}
