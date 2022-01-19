/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import static io.crums.sldg.sql.SqlTestHarness.*;
import static org.junit.Assert.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.sldg.SkipTable;


/**
 * 
 */
public class SqlTableAdaptorTest extends IoTestCase {
  
  
  @Test
  public void testEmpty() throws Exception {
    final Object label = new Object() { };
    try (SqlSkipTable table = newTable(label)) {
      assertEquals(0, table.size());
    }
    
  }
  

  @Test
  public void testOne() throws Exception {
    final Object label = new Object() { };
    
    Random rand = new Random(11);
    byte[] data = new byte[SkipTable.ROW_WIDTH];
    ByteBuffer dataBuf = ByteBuffer.wrap(data);
    
    try (SqlSkipTable table = newTable(label)) {
      
      rand.nextBytes(data);
      table.addRows(dataBuf.clear(), 0);
      
      assertEquals(1, table.size());
      ByteBuffer out = table.readRow(0);
      assertEquals(dataBuf.clear(), out);
    }
    
  }
  
  
  
  
  private SqlSkipTable newTable(Object label) throws ClassNotFoundException, SQLException {
    File dir = getMethodOutputFilepath(label);
    return newAdaptor(dir);
  }

}
