/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.awt.Color;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.lowagie.text.Font;

import io.crums.sldg.reports.pdf.Align;
import io.crums.sldg.reports.pdf.CellDataProvider.DateProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.ImageProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.StringProvider;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.sldg.reports.pdf.SourcedCell;
import io.crums.sldg.reports.pdf.SourcedCell.DateCell;
import io.crums.sldg.reports.pdf.SourcedCell.MultiStringCell;
import io.crums.sldg.reports.pdf.SourcedCell.NumberCell;
import io.crums.sldg.reports.pdf.SourcedCell.SourcedImage;
import io.crums.sldg.reports.pdf.SourcedCell.StringCell;
import io.crums.sldg.reports.pdf.SourcedCell.Sum;
import io.crums.sldg.reports.pdf.func.NumNode;
import io.crums.sldg.reports.pdf.func.NumFunc;
import io.crums.sldg.reports.pdf.func.NumOp;

/**
 * 
 */
public class SourcedCellParserTest extends ParserRoundtripTest.Base<SourcedCell> {

  public SourcedCellParserTest() {
    super(SourcedCellParser.INSTANCE);
  }
  
  
  @Test
  public void testString() throws Exception {
    clearPrint();
    var provider = new StringProvider("# ", " @!");
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    testRoundtrip(new StringCell(2, provider, null));
//    methodLabel = new Object() { };
    testRoundtrip(new StringCell(3, provider, format));
  }
  
  
  @Test
  public void testNumber() throws Exception {
    clearPrint();
    var provider = new NumberProvider("###,###.##", "$");
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    NumFunc func;
    {
      var children = List.of(NumNode.newArgLeaf(), NumNode.newLeaf(100));
      func = new NumFunc(NumNode.newBranch(NumOp.DIVIDE, children));
    }
    testRoundtrip(new NumberCell(0, provider, null, null));
    testRoundtrip(new NumberCell(1, provider, null, format));
    testRoundtrip(new NumberCell(2, provider, func, null));
//    methodLabel = new Object() { };
    testRoundtrip(new NumberCell(1, provider, func, format));
  }
  
  
  @Test
  public void testMultiString() throws Exception {
    clearPrint();
    var provider = new StringProvider();
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    testRoundtrip(new MultiStringCell(List.of(3,4), provider, null));
    methodLabel = new Object() { };
    testRoundtrip(new MultiStringCell(List.of(3,4), provider, format).setSeparator(","));
  }
  
  @Test
  public void testImage() throws Exception {
    clearPrint();
    var provider = new ImageProvider(100, 75);
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    format.setAlignH(Align.H.CENTER);
    testRoundtrip(new SourcedImage(7, provider, null));
//    methodLabel = new Object() { };
    testRoundtrip(new SourcedImage(7, provider, format));
  }
  
  @Test
  public void testDate() throws Exception {
    clearPrint();
    var provider = new DateProvider("yyyy-MM-dd");
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.BOLD, Color.BLACK));
    testRoundtrip(new DateCell(5, provider, null));
//    methodLabel = new Object() { };
    testRoundtrip(new DateCell(5, provider, format));
  }
  
  
  @Test
  public void testSum() throws Exception {
    clearPrint();
    var provider = new NumberProvider("###,###.##", "$");
    var colFunc = NumFunc.biFunction(NumOp.MULTIPLY);
    var func = NumFunc.divideBy(100.0);
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.NORMAL, Color.BLACK));
    List<Integer> cols = List.of(3, 7);
    List<Integer> monCol = List.of(6);
    testRoundtrip(new Sum(cols, colFunc, provider, null, null));
    testRoundtrip(new Sum(monCol, null, provider, null, null));
    testRoundtrip(new Sum(cols, colFunc, provider, null, format));
    methodLabel = new Object() { };
    testRoundtrip(new Sum(cols, colFunc, provider, func, format));
  }

}
