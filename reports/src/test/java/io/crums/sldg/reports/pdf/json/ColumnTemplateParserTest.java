/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import com.lowagie.text.Font;

import java.awt.Color;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.ColumnTemplate;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.sldg.reports.pdf.SourcedCell;
import io.crums.sldg.reports.pdf.CellDataProvider.DateProvider;

/**
 * 
 */
public class ColumnTemplateParserTest extends ParserRoundtripTest.Base<ColumnTemplate> {


  public ColumnTemplateParserTest() {
    super(ColumnTemplateParser.INSTANCE);
  }
  
  
  @Test
  public void test00() throws Exception {
    clearPrint();
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.BOLD, Color.BLACK));
    var cellFormat = new CellFormat(new FontSpec("Helvetica", 10, Font.BOLD, Color.GRAY));
    
    var protoSrc = new SourcedCell.DateCell(5, new DateProvider("yyyy-MM-dd"), null);
    var protoSrc2 = new SourcedCell.DateCell(5, new DateProvider("yyyy-MM-dd"), cellFormat);
    
    testRoundtrip(new ColumnTemplate(format, null));
    testRoundtrip(new ColumnTemplate(format, protoSrc));
    methodLabel = new Object() { };
    testRoundtrip(new ColumnTemplate(format, protoSrc2));
    
  }

}
