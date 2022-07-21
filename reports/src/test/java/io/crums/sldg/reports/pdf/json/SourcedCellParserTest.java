/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import com.gnahraf.test.SelfAwareTestCase;
import com.lowagie.text.Font;

import java.awt.Color;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.Align;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.sldg.reports.pdf.SourcedCell;
import io.crums.sldg.reports.pdf.CellDataProvider.*;
import io.crums.sldg.reports.pdf.SourcedCell.*;
import io.crums.sldg.reports.pdf.model.func.NumNode;
import io.crums.sldg.reports.pdf.model.func.NumberFunc;
import io.crums.sldg.reports.pdf.model.func.NumberOp;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class SourcedCellParserTest extends SelfAwareTestCase implements ParserRoundtripTest<SourcedCell> {

  private Object methodLabel;
  private void clear() {
    methodLabel = null;
  }
  
  
  @Test
  public void testString() throws Exception {
    clear();
    var provider = new StringProvider("# ", " @!");
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    testRoundtrip(new StringCell(2, provider, null));
//    methodLabel = new Object() { };
    testRoundtrip(new StringCell(3, provider, format));
  }
  
  
  @Test
  public void testNumber() throws Exception {
    clear();
    var provider = new NumberProvider("###,###.##", "$");
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    NumberFunc func;
    {
      var children = List.of(NumNode.newArgLeaf(), NumNode.newLeaf(100));
      func = new NumberFunc(NumNode.newBranch(NumberOp.DIVIDE, children));
    }
    testRoundtrip(new NumberCell(0, provider, null, null));
    testRoundtrip(new NumberCell(1, provider, null, format));
    testRoundtrip(new NumberCell(2, provider, func, null));
//    methodLabel = new Object() { };
    testRoundtrip(new NumberCell(1, provider, func, format));
  }
  
  
  @Test
  public void testMultiString() throws Exception {
    clear();
    var provider = new StringProvider();
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    testRoundtrip(new MultiStringCell(List.of(3,4), provider, null));
//    methodLabel = new Object() { };
    testRoundtrip(new MultiStringCell(List.of(3,4), provider, format).setSeparator(","));
  }
  
  @Test
  public void testImage() throws Exception {
    clear();
    var provider = new ImageProvider(100, 75);
    var format = new CellFormat(new FontSpec("Helvetica", 9, Font.NORMAL, Color.GRAY));
    format.setAlignH(Align.H.CENTER);
    testRoundtrip(new SourcedImage(7, provider, null));
//    methodLabel = new Object() { };
    testRoundtrip(new SourcedImage(7, provider, format));
  }
  
  @Test
  public void testDate() throws Exception {
    clear();
    var provider = new DateProvider("yyyy-MM-dd");
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.BOLD, Color.BLACK));
    testRoundtrip(new DateCell(5, provider, null));
//    methodLabel = new Object() { };
    testRoundtrip(new DateCell(5, provider, format));
  }
  
  
  @Test
  public void testSum() throws Exception {
    clear();
    var provider = new NumberProvider("###,###.##", "$");
    var colFunc = NumberFunc.biFunction(NumberOp.MULTIPLY);
//        new NumberFunc(
//            NumNode.newBranch(NumberOp.MULTIPLY, List.of(NumNode.newArgLeaf(), NumNode.newArgLeaf())));
    var func = NumberFunc.divideBy(100.0);
    var format = new CellFormat(new FontSpec("Helvetica", 11, Font.NORMAL, Color.BLACK));
    List<Integer> cols = List.of(3, 7);
    List<Integer> monCol = List.of(6);
    testRoundtrip(new Sum(cols, colFunc, provider, null, null));
    testRoundtrip(new Sum(monCol, null, provider, null, null));
    testRoundtrip(new Sum(cols, colFunc, provider, null, format));
    methodLabel = new Object() { };
    testRoundtrip(new Sum(cols, colFunc, provider, func, format));
  }
  
 
  
  @Override
  public JsonEntityParser<SourcedCell> parser() {
    return SourcedCellParser.INSTANCE;
  }
  

  
  @Override
  public void observeJson(JSONObject jObj, SourcedCell expected) {
    if (methodLabel == null)
      return;
    System.out.println(" -- " + method(methodLabel) + " --");
    JsonPrinter.println(jObj);
  }

}
