/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.sldg.src.ColumnInfo;
import io.crums.sldg.src.SourceInfo;
import io.crums.util.Lists;
import io.crums.util.json.JsonPrinter;

/**
 * 
 */
public class SourceInfoParserTest extends SelfAwareTestCase {

  
  @Test
  public void testMinRoundtrip() {
    String name = "Acme Warehouse";
    String desc = null;
    List<ColumnInfo> columns = null;
    var expected = new SourceInfo(name, desc, columns);
    testRoundtrip(expected);
  }
  

  @Test
  public void testRoundtrip02() {
    String name = "Acme Warehouse";
    String desc = "Ledger of goods";
    List<ColumnInfo> columns = null;
    var expected = new SourceInfo(name, desc, columns);
    testRoundtrip(expected);
  }
  

  @Test
  public void testRoundtrip03() {
    String name = "Acme Warehouse";
    String desc = null;
    List<ColumnInfo> columns;
    {
      ColumnInfo[] cols = {
          new ColumnInfo("TID", 1, "transaction ID", null),
      };
      
      columns = Lists.asReadOnlyList(cols);
    }
    var expected = new SourceInfo(name, desc, columns);
    testRoundtrip(expected);
  }
  

  @Test
  public void testRoundtrip04() {
    String name = "Acme Warehouse";
    String desc = "Ledger of goods in n out (+/-)";
    List<ColumnInfo> columns;
    {
      ColumnInfo[] cols = {
          new ColumnInfo("Carrots", 5, "carrots", "carrot"),
      };
      
      columns = Lists.asReadOnlyList(cols);
    }
    var expected = new SourceInfo(name, desc, columns);
    testRoundtrip(expected);
  }
  

  @Test
  public void testRoundtrip05() {
    String name = "Acme Warehouse";
    String desc = "Ledger of \"goods\" in n out (+/-)";
    List<ColumnInfo> columns = null;
    {
      ColumnInfo[] cols = {
          new ColumnInfo("TID", 1, "transaction ID", null),
          new ColumnInfo("Carrots", 5, "unwashed carrots", "carrot"),
          new ColumnInfo("DS", 6, "dynamite sticks", "4-pack"),
      };
      
      columns = Lists.asReadOnlyList(cols);
    }
    String dateFormat = "EEE, d MMM yyyy HH:mm:ss Z z";
    var expected = new SourceInfo(name, desc, columns, dateFormat);
    testRoundtrip(expected, new Object() { });
  }
  
  
  private void testRoundtrip(SourceInfo expected) {
    testRoundtrip(expected, null);
  }
  
  private void testRoundtrip(SourceInfo expected, Object label) {
    var parser = SourceInfoParser.INSTANCE;
    var jObj = parser.toJsonObject(expected);
    String json = JsonPrinter.toJson(jObj);
    if (label != null) {
      System.out.println("=== " + method(label) + " ===");
      System.out.println(json);
      System.out.println();
    }
    SourceInfo readBack = parser.toEntity(json);
    assertEquals(expected.getName(), readBack.getName());
    assertEquals(expected.getDescription(), readBack.getDescription());
    assertEquals(expected.getColumnInfoCount(), readBack.getColumnInfoCount());
    var expectedCols = expected.getColumnInfos();
    var actualCols = readBack.getColumnInfos();
    for (int index = expectedCols.size(); index-- > 0; )
      ColumnInfoParserTest.assertEqual(
          expectedCols.get(index), actualCols.get(index));
    
    assertEquals(expected.getDateFormatPattern(), readBack.getDateFormatPattern());
  }

}
