/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.awt.Color;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.lowagie.text.Font;

import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.ColumnTemplate;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.sldg.reports.pdf.LineSpec;
import io.crums.sldg.reports.pdf.SourcedCell;
import io.crums.sldg.reports.pdf.TableTemplate;
import io.crums.sldg.reports.pdf.Align.H;
import io.crums.sldg.reports.pdf.CellData.TextCell;
import io.crums.sldg.reports.pdf.CellDataProvider.DateProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.StringProvider;
import io.crums.sldg.reports.pdf.func.BaseNumFunc;
import io.crums.sldg.reports.pdf.func.NumOp;
import io.crums.sldg.reports.pdf.json.ParserRoundtripTest.Base;

/**
 * 
 */
public class TableTemplateParserTest extends Base<TableTemplate> {

  
  /**
   * 
   */
  public TableTemplateParserTest() {
    super(TableTemplateParser.INSTANCE);
  }
  
  
  @Test
  public void testSingleColumn() throws Exception {
    clearPrint();
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.NORMAL, Color.BLACK));
    var cellFormat = new CellFormat(new FontSpec("Helvetica", 10, Font.BOLD, Color.GRAY));
    
    var protoSrc = new SourcedCell.DateCell(5, new DateProvider("yyyy-MM-dd"), null);
    
    
    var table = new TableTemplate(List.of(new ColumnTemplate(format, null)));
    
    testRoundtrip(table);
//    methodLabel = new Object() { };
    table = new TableTemplate(List.of(new ColumnTemplate(format, protoSrc)));
    testRoundtrip(table);
  }
  
  
  @Test
  public void testDoubleColumn() throws Exception {
    clearPrint();
//    methodLabel = new Object() { };
    
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.NORMAL, Color.BLACK));
    var cellFormat = new CellFormat(new FontSpec("Helvetica", 10, Font.BOLD, Color.GRAY));
    
    var protoSrc = new SourcedCell.DateCell(5, new DateProvider("yyyy-MM-dd"), null);
    var protoSrc2 = new SourcedCell.NumberCell(
        2,
        new NumberProvider("###,###.##", "$"),
        BaseNumFunc.divideBy(100.0),
        cellFormat);
    
    var table = new TableTemplate(
        List.of(
            new ColumnTemplate(format, protoSrc),
            new ColumnTemplate(format, protoSrc2)));
            
    testRoundtrip(table); 
  }
  
  
  @Test
  public void testWithFixed() throws Exception {
    clearPrint();
    methodLabel = new Object() { };
    
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.NORMAL, Color.BLACK));
    format.setPadding(4);
    var cellFormat = new CellFormat(new FontSpec("Helvetica", 10, Font.BOLD, Color.GRAY));
    

    var trackIdCell = new SourcedCell.StringCell(2, new StringProvider(), null);
    var unitPriceCell = new SourcedCell.NumberCell(3, new NumberProvider("###,###.##", "$"), null, null);
    var quantityCell = new SourcedCell.NumberCell(4, new NumberProvider(), null, null);
    
    ColumnTemplate[] columns = {
        new ColumnTemplate(cellFormat, trackIdCell),
        new ColumnTemplate(cellFormat, unitPriceCell),
        new ColumnTemplate(cellFormat, quantityCell)
    };
    
    var mainTable = new TableTemplate(List.of(columns));

    var mainBorderColor = new Color(100, 183, 222);
    mainTable.setTableBorders(new LineSpec(3, mainBorderColor));
    var headingStyle = new CellFormat(new FontSpec("Helvetica", 10, Font.BOLD, Color.WHITE));
    headingStyle.setBackgroundColor(mainBorderColor);
    headingStyle.setAlignH(H.CENTER);
    headingStyle.setPadding(8);
    CellData[] colHeadings = {
        new TextCell("Track ID", headingStyle),
        new TextCell("Unit Price", headingStyle),
        new TextCell("Quantity", headingStyle),
    };
    for (int index = 0; index < colHeadings.length; ++index)
      mainTable.setFixedCell(index, colHeadings[index]);

    var total = new SourcedCell.Sum(
        List.of(3,4),
        BaseNumFunc.biFunction(NumOp.MULTIPLY),
        new NumberProvider("###,###.##", "$"), null, null);

    mainTable.setFixedCell(1, -2, new TextCell("Total: "));
    mainTable.setFixedCell(2, -2, total);
    
    testRoundtrip(mainTable);
    
  }
  
  
  
  

}





