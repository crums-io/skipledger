/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.sldg.db.VolatileTable;

/**
 * 
 */
public class PathTest extends SelfAwareTestCase {
  
  
  
  @Test
  public void testSerializeAndMinimal() throws Exception {
    final int rows = 79;
    long[] rowNumbers = { 11 };
    testSerializeAndLoad(rows, rowNumbers);
  }
  
  
  @Test
  public void testSerializeAndLoad() throws Exception {
    final int rows = 13;
    long[] rowNumbers = { 7, 8, 10, 11 };
    testSerializeAndLoad(rows, rowNumbers);
  }
  
  
  private void testSerializeAndLoad(int size, long[] pathNumbers) throws Exception {

    Ledger ledger = newRandomLedger(size);
    
    ArrayList<Row> rows = new ArrayList<>();
    
    for (long n : pathNumbers)
      rows.add(ledger.getRow(n));
    
    Path path = new Path(rows);
    
    ByteBuffer buffer = path.serialize();
    
    int flack = 7;  // make sure it's self-delimiting
    byte[] serialized = new byte[buffer.remaining() + flack];
    buffer.get(serialized, 0, buffer.remaining());
    
    InputStream in = new ByteArrayInputStream(serialized);
    
    Path out = Path.load(in);
    
    assertEquals(path.path(), out.path());
    
    in.close();
    ledger.close();
  }
  
  
  
  
  public static Ledger newRandomLedger(int rows) {
    if (rows <= 0)
      throw new IllegalArgumentException("rows " + rows);
    
    Ledger ledger = new CompactLedger(new VolatileTable());
    
    Random random = new Random(rows);

    byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
    ByteBuffer mockHashBuffer = ByteBuffer.wrap(mockHash);
    
    for (int count = rows; count-- > 0; ) {
      random.nextBytes(mockHash);
      mockHashBuffer.clear();
      ledger.appendRows(mockHashBuffer);
    }
    
    assertEquals(rows, ledger.size());
    return ledger;
  }

}
