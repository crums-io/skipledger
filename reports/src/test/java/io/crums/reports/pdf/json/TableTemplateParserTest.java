/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;
import java.util.List;

import io.crums.reports.pdf.Align;
import io.crums.reports.pdf.CellFormat;
import io.crums.reports.pdf.FontSpec;
import io.crums.reports.pdf.LineSpec;
import io.crums.reports.pdf.TableTemplate;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.SelfAwareTestCase;
import com.lowagie.text.Font;

/**
 * 
 */
public class TableTemplateParserTest extends SelfAwareTestCase implements ParserRoundtripTest<TableTemplate> {
  
  private Object methodLabel;
  
  @Test
  public void singleColumn() throws Exception {
    
    methodLabel = new Object() {  };
    
    var font = new FontSpec("Helvetica", 10, Font.BOLD, Color.BLACK);
    
    var col = new CellFormat(font);
    
    var table = new TableTemplate(List.of(col));  // single column
    
    testRoundtrip(table);
  }
  
  
  @Test
  public void test2SameColumns() throws Exception {
    
    methodLabel = new Object() {  };
    
    var font = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
    
    var col = new CellFormat(font);
    
    var table = new TableTemplate(List.of(col, col));
    
    testRoundtrip(table);
    
  }
  
  
  @Test
  public void test2UniqueColumns() throws Exception {
    
    methodLabel = new Object() {  };
    
    var font = new FontSpec("Helvetica", 10, Font.ITALIC, Color.BLACK);
    
    var col1 = new CellFormat(font);
    
    col1.setLeading(1.5f);
    col1.scalePaddingToFont(1.0f);
    
    var col2 = new CellFormat(new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK));
    
    
    var table = new TableTemplate(List.of(col1, col2));
    
    testRoundtrip(table);
    
  }
  
  
  @Test
  public void test2UniqueColsWithMoreSettings() throws Exception {
    
    methodLabel = new Object() {  };
    
    var font = new FontSpec("Helvetica", 10, Font.ITALIC, Color.BLACK);
    
    var col1 = new CellFormat(font);
    
    col1.setLeading(1.5f);
    col1.scalePaddingToFont(1.0f);
    
    var col2 = new CellFormat(new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK));
    col2.scalePaddingToFont(1.0f);
    col2.setAlignH(Align.H.RIGHT);
    
    
    var table = new TableTemplate(List.of(col1, col2));
    
    table.setTableBorders(new LineSpec(1.5f, Color.BLACK));
    table.setDefaultLines(new LineSpec(1, Color.BLACK));
    
    testRoundtrip(table);
    
  }
  
  

  @Test
  public void test2UniqueCols80x20() throws Exception {
    
    methodLabel = new Object() {  };
    
    var font = new FontSpec("Helvetica", 10, Font.ITALIC, Color.BLACK);
    
    var col1 = new CellFormat(font);
    
    col1.setLeading(1.5f);
    col1.scalePaddingToFont(1.0f);
    
    var col2 = new CellFormat(new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK));
    col2.scalePaddingToFont(1.0f);
    col2.setAlignH(Align.H.RIGHT);
    
    
    var table = new TableTemplate(List.of(col1, col2));
    table.setColumnWidths(0.8f, 0.2f);
    
    table.setTableBorders(new LineSpec(1.5f, Color.BLACK));
    table.setDefaultLines(new LineSpec(1, Color.BLACK));
    
    testRoundtrip(table);
    
  }
  
  
  
  
  
  @Override
  public void observeJson(JSONObject jObj, TableTemplate expected) {
    if (methodLabel != null) {
      var out = System.out;
      out.println(" - - - " + method(methodLabel) + " - - -");
      new JsonPrinter(out).print(jObj);
      out.println();
    }
  }
  

  @Override
  public JsonEntityParser<TableTemplate> parser() {
    return TableTemplateParser.INSTANCE;
  }

  @Override
  public void assertTripEquals(TableTemplate expected, TableTemplate actual) {
    
    assertTrue(TableTemplate.equal(expected, actual));
  }

}
