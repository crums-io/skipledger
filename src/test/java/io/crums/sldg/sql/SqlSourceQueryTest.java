/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Random;

import static io.crums.sldg.sql.SqlTestHarness.*;

import  org.junit.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.LongValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;

/**
 * 
 */
public class SqlSourceQueryTest extends IoTestCase {
  
  
  @Test
  public void testOne() throws Exception {
    Object label = new Object() { };
    testOneImpl(TableSalt.NULL_SALT, label);
  }
  
  @Test
  public void testOneSalted() throws Exception {
    Object label = new Object() { };
    testOneImpl(randomShaker(1), label);
  }
  
  
  private TableSalt randomShaker(long init) {

    Random random = new Random(init);
    byte[] seed = new byte[SldgConstants.HASH_WIDTH];
    random.nextBytes(seed);
    return new TableSalt(seed);
  }
  
  
  private void testOneImpl(TableSalt shaker, Object label) throws Exception {

    String createSql =
        "CREATE TABLE inventory (\n" +
        "  entry_no BIGINT NOT NULL AUTO_INCREMENT,\n" +
        "  entry_type CHAR(2) NOT NULL,\n" +
        "  serial_no_1 INT,\n" +
        "  serial_no_2 INT,\n" +
        "  entry_date DATE,\n" +
        "  entry_name VARCHAR(240) NOT NULL,\n" +
        "  PRIMARY KEY (entry_no)\n" +
        ")";
    
    File dir = getMethodOutputFilepath(label);
    
    try (Connection con = newDatabase(dir)) {
      
      con.createStatement().execute(createSql);
      
      SqlSourceQuery srcLedger = new SqlSourceQuery.Builder(
          "inventory",
          "entry_no", "entry_type", "serial_no_1", "serial_no_2", "entry_date", "entry_name").build(con, shaker);

      
      assertEquals(0, srcLedger.size());
      
      // add a row
      
      String insertSql =
          "INSERT INTO inventory (entry_type, serial_no_1, entry_name) VALUES\n" +
          "('CD', 53180022, 'V 3.125 AFR 90')";
      
      
      Statement stmt = con.createStatement();
      stmt.execute(insertSql);
      assertEquals(1, stmt.getUpdateCount());
      
      assertEquals(1L, srcLedger.size());
      
      SourceRow srcRow = srcLedger.getSourceByRowNumber(1);
      assertEquals(1, srcRow.rowNumber());
      assertEquals(6, srcRow.getColumns().size());
      
      assertNumberValue(srcRow, 0, 1);
      assertStringValue(srcRow, 1, "CD");
      assertNumberValue(srcRow, 2, 53180022);
      assertNullValue(srcRow, 3);
      assertNullValue(srcRow, 4);
      assertStringValue(srcRow, 5, "V 3.125 AFR 90");
    }
  }
  
  
  @Test
  public void testTwo() throws Exception {
    final Object label = new Object() { };
    String createSql =
        "CREATE TABLE inventory (\n" +
        "  entry_no BIGINT NOT NULL AUTO_INCREMENT,\n" +
        "  entry_type CHAR(2) NOT NULL,\n" +
        "  serial_no_1 INT,\n" +
        "  serial_no_2 INT,\n" +
        "  entry_date DATE,\n" +
        "  entry_name VARCHAR(240) NOT NULL,\n" +
        "  PRIMARY KEY (entry_no)\n" +
        ")";
    
    File dir = getMethodOutputFilepath(label);
    
    try (Connection con = newDatabase(dir)) {
      
      con.createStatement().execute(createSql);
      
      SqlSourceQuery srcLedger = new SqlSourceQuery.Builder(
          "inventory",
          "entry_no", "entry_type", "serial_no_1", "serial_no_2", "entry_date", "entry_name").build(con);

      
      assertEquals(0, srcLedger.size());
      
      // add a row
      
      String insertSql =
          "INSERT INTO inventory (entry_type, serial_no_1, entry_name) VALUES\n" +
          "('CD', 53180022, 'V 3.125 AFR 90')";
      
      
      Statement stmt = con.createStatement();
      stmt.execute(insertSql);
      assertEquals(1, stmt.getUpdateCount());
      
      assertEquals(1L, srcLedger.size());
      
      insertSql =
          "INSERT INTO inventory (entry_type, serial_no_1, entry_date, entry_name) VALUES\n" +
          "('KG', 5317400, DATE '2021-06-10', 'K 6x Muha')";
      
      stmt.execute(insertSql);
      assertEquals(1, stmt.getUpdateCount());
      
      assertEquals(2L, srcLedger.size());
      
      SourceRow srcRow = srcLedger.getSourceByRowNumber(1);
      assertEquals(1, srcRow.rowNumber());
      assertEquals(6, srcRow.getColumns().size());
      
      assertNumberValue(srcRow, 0, 1);
      assertStringValue(srcRow, 1, "CD");
      assertNumberValue(srcRow, 2, 53180022);
      assertNullValue(srcRow, 3);
      assertNullValue(srcRow, 4);
      assertStringValue(srcRow, 5, "V 3.125 AFR 90");
      
      srcRow = srcLedger.getSourceByRowNumber(2);
      assertEquals(2, srcRow.rowNumber());
      assertEquals(6, srcRow.getColumns().size());
      
      assertNumberValue(srcRow, 0, 2);
      assertStringValue(srcRow, 1, "KG");
      assertNumberValue(srcRow, 2, 5317400);
      assertNullValue(srcRow, 3);
      
      LongValue dateValue = (LongValue) srcRow.getColumns().get(4);
      // don't want to get bogged down in timezone issues, so just check the UTC time is in the
      // right range
      Calendar min = Calendar.getInstance();
      min.clear();
      min.set(2021, 5, 9);
      
      Calendar max = Calendar.getInstance();
      max.clear();
      max.set(2021, 5, 11);
      
      assertTrue(dateValue.getNumber() > min.getTimeInMillis());
      assertTrue(dateValue.getNumber() < max.getTimeInMillis());
      

      assertStringValue(srcRow, 5, "K 6x Muha");
    }
  }
  
  
  private void assertNumberValue(SourceRow srcRow, int index, long value) {
    assertEquals(ColumnType.LONG, srcRow.getColumns().get(index).getType());
    assertEquals(value, ((LongValue) srcRow.getColumns().get(index)).getNumber());
  }
  
  private void assertStringValue(SourceRow srcRow, int index, String value) {
    assertEquals(ColumnType.STRING, srcRow.getColumns().get(index).getType());
    assertEquals(value, ((StringValue) srcRow.getColumns().get(index)).getString());
  }
  
  
  private void assertNullValue(SourceRow srcRow, int index) {
    assertEquals(ColumnType.NULL, srcRow.getColumns().get(index).getType());
  }
  

}
