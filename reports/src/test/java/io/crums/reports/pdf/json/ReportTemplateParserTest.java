/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static org.junit.Assert.*;

import java.awt.Color;
import java.util.List;

import org.junit.Test;

import com.lowagie.text.Font;

import io.crums.reports.pdf.Align;
import io.crums.reports.pdf.BorderContent;
import io.crums.reports.pdf.CellData;
import io.crums.reports.pdf.CellFormat;
import io.crums.reports.pdf.FixedTable;
import io.crums.reports.pdf.FontSpec;
import io.crums.reports.pdf.Header;
import io.crums.reports.pdf.LineSpec;
import io.crums.reports.pdf.ReportTemplate;
import io.crums.reports.pdf.ReportTemplateTest;
import io.crums.reports.pdf.TableTemplate;
import io.crums.util.json.JsonEntityParser;

/**
 * 
 */
public class ReportTemplateParserTest extends RefedImageParserTest<ReportTemplate> {

  
  private final static String[] IMAGE_RES = { ReportTemplateTest.ICON };
  
  
  
  @Test
  public void testWithIcon() throws Exception {
//    methodLabel = new Object() {  };
    
    var iconCell = CellData.forImage(IMAGE_RES[0], loadResourceBytes(IMAGE_RES[0]).array(), 50, 50);
    var headerTable = new FixedTable(3, 3, new FontSpec("Helvetica", 9, Font.NORMAL, Color.BLACK));
    headerTable.setFixedCell(0, 1, iconCell);
    headerTable.setDefaultCell(CellData.TextCell.BLANK);
    headerTable.setColumnWidths(0.25f, 0.5f, 0.25f);
    var mainTable = ReportTemplateTest.newTableTemplate(ReportTemplateTest.MAIN_TABLE_DEF);
    var footer = new BorderContent(
        "© 2022 example.com",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Header(headerTable, null), mainTable, footer);
    testRoundtrip(report);
  }
  

  @Test
  public void testWithReferences() throws Exception {
    methodLabel = new Object() {  };
    
    var references = new EditableRefContext();
    
    var hdrFontColor = new Color(24, 24, 56);
    references.colorRefs().put("hdrTheme", hdrFontColor);
    references.colorRefs().put("gray", Color.GRAY);
    
    var hdrFont = new FontSpec("Helvetica", 9, Font.NORMAL, Color.BLACK);
    references.fontRefs().put("hdrTheme", hdrFont);
    
    var headerTable = new FixedTable(3, 3, hdrFont);
    
    var iconCell = CellData.forImage(IMAGE_RES[0], loadResourceBytes(IMAGE_RES[0]).array(), 50, 50);
    references.cellDataRefs().put(IMAGE_RES[0], iconCell);
    
    headerTable.setFixedCell(0, 1, iconCell);
    headerTable.setDefaultCell(CellData.TextCell.BLANK);
    
    headerTable.setColumnWidths(0.25f, 0.5f, 0.25f);
    
    
    var mainDescFont = new FontSpec("Helvetica", 10, Font.ITALIC, Color.BLACK);
    references.fontRefs().put("mainDesc", mainDescFont);
    var mainFigureFont = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
    references.fontRefs().put("mainFig", mainFigureFont);
    
    // construct a 2-column, 80 x 20 main table
    
    var col1 = new CellFormat(mainDescFont);
    
    col1.setLeading(1.5f);
    col1.scalePaddingToFont(1.0f);
    
    references.cellFormatRefs().put("mainColLeft", col1);
    
    var col2 = new CellFormat(mainFigureFont);
    col2.scalePaddingToFont(1.0f);
    col2.setAlignH(Align.H.RIGHT);

    references.cellFormatRefs().put("mainColRight", col2);
    
    var mainTable = new TableTemplate(List.of(col1, col2));
    mainTable.setColumnWidths(0.8f, 0.2f);
    
    mainTable.setTableBorders(new LineSpec(1.5f, hdrFontColor));
    mainTable.setDefaultLines(new LineSpec(1, hdrFontColor));
    
    var footerFont = new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY);
    references.fontRefs().put("ftrTheme", footerFont);
    
    var footer = new BorderContent(
        "© 2022 example.com",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY),
        Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Header(headerTable, null), mainTable, footer);
    report.setReferences(references);
    
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
