/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import static io.crums.sldg.reports.pdf.ReportTemplateTest.*;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.ReportTemplateTest;
import io.crums.testing.IoTestCase;

import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

/**
 * Leans on {@linkplain ReportTemplateTest}
 */
public class ReportTemplateParserTest extends IoTestCase {
  
  
  public final static String QUERY_TOTAL_RES = "queryTotal.json";

  
  @Test
  public void testQueryTotal() {
    
    var report = queryWithTotalInstance();
    var parser = new ReportTemplateParser().setRefedImages(iconMap());
    var jReport = parser.toJsonObject(report);
//    JsonPrinter.println(jReport);
    var genReport = parser.toEntity(jReport);
    
    assertEquals(report, genReport);

    // 
    final Object label = new Object() {  };
    writeToFile(jReport, label);
  }
  
  
  @Test
  public void testQueryTotalWithAutoRef() {
    
    var report = queryWithTotalInstance();
    
    var parser = new ReportTemplateParser().setRefedImages(iconMap()).setReferences(new AutoRefContext());
    
    var jReport = parser.toJsonObject(report);
    
//    JsonPrinter.println(jReport);
    parser.setReferences(null);
    
    var genReport = parser.toEntity(jReport);
    
    assertEquals(report, genReport);
    
  }
  
  
  @Test
  public void testQueryTotalWithAutoRef2x() throws Exception {

    final Object label = new Object() {  };
    
    var report = queryWithTotalInstance();
    
    var parser = new ReportTemplateParser().setRefedImages(iconMap()).setReferences(new AutoRefContext());
    parser.toJsonObject(report);  // for side effects
    
    var gatheredRefs = new EditableRefContext(parser.getReferences());
    parser.setReferences(gatheredRefs);

    var jReport = parser.toJsonObject(report);
    
    
//    JsonPrinter.println(jReport);
    
    parser.setReferences(null);
    
    var genReport = parser.toEntity(jReport);
    
    assertEquals(report, genReport);
    
    writeToFile(jReport, label, "autoRef2x.json");
  }
  
  
  private void writeToFile(JSONObject jObj, Object label) {
    writeToFile(jObj, label, null);
  }
  
  private void writeToFile(JSONObject jObj, Object label, String filename) {
    var dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assert dir.isDirectory();
    if (filename == null)
      filename = method(label) + ".json";
    var jsonFile = new File(dir, filename);
    JsonPrinter.write(jObj, jsonFile);
  }
  
  
  
  @Test
  public void testQueryTotalFromResource() throws Exception {
    
    final Object label = new Object() {  };
    
    var parser = new ReportTemplateParser().setRefedImages(iconMap());
    var report = parser.toEntity(getClass().getResourceAsStream(QUERY_TOTAL_RES));
    
    var dir = getMethodOutputFilepath(label);
    
    assertTrue(dir.mkdirs());
    
    File pdfFile = new File(dir, method(label) + ".pdf");
    
    assertEquals(1, report.getNumberArgs().size());
    report.getNumberArgs().get(0).set(INVOICE_47);
    
    // FIXME: Uncomment after resource fix
    // var sourceRows = getChinookMorsel717().sources();
    // report.writePdf(pdfFile, sourceRows);
  }
  
  

}
