/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static org.junit.Assert.*;

import java.awt.Color;

import org.junit.Test;

import com.lowagie.text.Font;

import io.crums.reports.pdf.Align;
import io.crums.reports.pdf.BorderContent;
import io.crums.reports.pdf.CellData;
import io.crums.reports.pdf.FixedTable;
import io.crums.reports.pdf.FontSpec;
import io.crums.reports.pdf.Header;
import io.crums.reports.pdf.ReportTemplate;
import io.crums.reports.pdf.ReportTemplateTest;
import io.crums.util.json.JsonEntityParser;

/**
 * 
 */
public class ReportTemplateParserTest extends RefedImageParserTest<ReportTemplate> {

  
  private final static String[] IMAGE_RES = { ReportTemplateTest.ICON };
  
  
  
  @Test
  public void testWithIcon() throws Exception {
    methodLabel = new Object() {  };
    
    var iconCell = CellData.forImage(IMAGE_RES[0], loadResourceBytes(IMAGE_RES[0]).array(), 50, 50);
    var headerTable = new FixedTable(3, 3, new FontSpec("Helvetica", 9, Font.NORMAL, Color.BLACK));
    headerTable.setFixedCell(0, 1, iconCell);
    headerTable.setDefaultCell(CellData.TextCell.BLANK);
    headerTable.setColumnWidths(0.25f, 0.5f, 0.25f);
    var mainTable = ReportTemplateTest.newTableTemplate(ReportTemplateTest.MAIN_TABLE_DEF);
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Header(headerTable, null), mainTable, footer);
    testRoundtrip(report);
  }
  

  @Test
  public void testWithBlueBar() throws Exception {
    methodLabel = new Object() {  };
    
    var iconCell = CellData.forImage(IMAGE_RES[0], loadResourceBytes(IMAGE_RES[0]).array(), 50, 50);
    var headerTable = new FixedTable(3, 3, new FontSpec("Helvetica", 9, Font.NORMAL, Color.BLACK));
    headerTable.setFixedCell(0, 1, iconCell);
    headerTable.setDefaultCell(CellData.TextCell.BLANK);
    headerTable.setColumnWidths(0.25f, 0.5f, 0.25f);
    var mainTable = ReportTemplateTest.newTableTemplate(ReportTemplateTest.MAIN_TABLE_DEF);
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Header(headerTable, null), mainTable, footer);
    testRoundtrip(report);
  }
  
  
  
  
  @Override
  public JsonEntityParser<ReportTemplate> parser() throws Exception {
    var refedImages = getRefedImages(IMAGE_RES);
    return new ReportTemplateParser(refedImages);
  }


  @Override
  public void assertTripEquals(ReportTemplate expected, ReportTemplate actual) {
    assertTrue(ReportTemplate.equal(expected, actual));
  }
  
  
  

}
