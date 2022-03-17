/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.lowagie.text.Font;

import io.crums.reports.pdf.CellData;
import io.crums.reports.pdf.FixedTable;
import io.crums.reports.pdf.FontSpec;
import io.crums.reports.pdf.ReportTemplateTest;
import io.crums.util.json.JsonEntityParser;

/**
 * 
 */
public class FixedTableParserTest extends RefedImageParserTest<FixedTable> {
  
  private final static String[] IMAGE_RES = { ReportTemplateTest.ICON };
  
  
  
  @Test
  public void test3x3() throws Exception {
    methodLabel = new Object() {  };
    var fixedTable = new FixedTable(3, 3, new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK));
    fixedTable.setColumnWidths(0.25f, 0.5f, 0.25f);
    fixedTable.setFixedCell(
        0, 1,
        CellData.forImage(IMAGE_RES[0], loadResourceBytes(IMAGE_RES[0]).array(), 50, 52));
    fixedTable.setFixedCell(2, 0, CellData.forText("Crums Test Report"));
    testRoundtrip(fixedTable);
  }

  
  @Test
  public void test3x3Fixed() throws Exception {
    methodLabel = new Object() {  };
    var fixedTable = new FixedTable(3, 3, new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK));
    fixedTable.setColumnWidths(0.25f, 0.5f, 0.25f);
    fixedTable.setFixedCell(
        0, 1,
        CellData.forImage(IMAGE_RES[0], loadResourceBytes(IMAGE_RES[0]).array(), 50, 50));
    fixedTable.setFixedCell(2, 0, CellData.forText("Crums Test Report"));
    fixedTable.setDefaultCell(CellData.forText(" \n "));
    testRoundtrip(fixedTable);
  }
  
  

  @Override
  public void assertTripEquals(FixedTable expected, FixedTable actual) {
    assertTrue(FixedTable.equal(expected, actual));
    assertEquals(expected.getDefaultCell(), actual.getDefaultCell());
    assertEquals(expected.getDynamicCellCount(), actual.getDynamicCellCount());
    assertEquals(expected.getFixedCells(), actual.getFixedCells());
  }

  @Override
  public JsonEntityParser<FixedTable> parser() throws IOException {
    var refedImages = getRefedImages(IMAGE_RES);
    return new FixedTableParser(refedImages);
  }


}
