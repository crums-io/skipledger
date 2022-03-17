/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf;


import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;
import com.lowagie.text.Font;
import com.lowagie.text.Image;

import io.crums.reports.pdf.json.ReportTemplateParser;
import io.crums.reports.pdf.json.TableTemplateParser;

/**
 * 
 */
public class ReportTemplateTest extends IoTestCase {
  
  
  public final static String RPT_W_ICON = "report_with_refs.json";

  public final static String MAIN_TABLE_DEF = "tt_rpt.json";
  
  public final static String ICON = "example_icon.png";
  
  @Test
  public void test00() throws IOException {
    var label = new Object() {  };
    
    var icon = loadImage(ICON);
    
    CellData iconCell = CellData.forImage(ICON, icon, 50, 50);
    var headerTable = new FixedTable(3, 3, new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK));
    headerTable.setFixedCell(0, 1, iconCell);
    headerTable.setColumnWidths(0.25f, 0.5f, 0.25f);
    headerTable.setRemainingCells(CellData.TextCell.BLANK);
    var mainTable = newTableTemplate(MAIN_TABLE_DEF);
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    
    var report = new ReportTemplate(new Header(headerTable, null), mainTable, footer);
    String[] inputs = {
        "Acme Widget Model A", "$38.29",
        "Zorborg Cayote Adapter", "$16.70"
    };
    var file = newPdfPath(label);
    List<CellData> cells = CellData.forText(List.of(inputs));
    report.writePdfFile(file, cells);
  }
  
  
  @Test
  public void testFromResource() throws IOException {
    var label = new Object() {  };
    var refedImages = getRefedImages(ICON);
    var report =
        new ReportTemplateParser(refedImages).toEntity(
            getClass().getResourceAsStream(RPT_W_ICON));
    String[] inputs = {
        "Acme Widget Model A", "$38.29",
        "Zorborg Cayote Adapter", "$16.70"
    };
    var file = newPdfPath(label);
    List<CellData> cells = CellData.forText(List.of(inputs));
    report.writePdfFile(file, cells);
  }
  
  
  @Test
  public void customInputExperiment() throws IOException {
    var label = new Object() {  };
    var refedImages = getRefedImages(ICON);
    var report =
        new ReportTemplateParser(refedImages).toEntity(
            getClass().getResourceAsStream(RPT_W_ICON));
    String[] inputs = {
        "Acme Widget Model A", "$38.29",
        "Zorborg Cayote Adapter", "$16.70"
    };
    
    var bgColor = new Color(100, 183, 222);
    var cellFormat = new CellFormat(new FontSpec("Helvetica", 14, 0, bgColor));
    cellFormat.setBackgroundColor(bgColor);
    var borderCell = new CellData.TextCell(" ", cellFormat);

    var fancyInputs = new ArrayList<CellData>();
    fancyInputs.add(borderCell);
    fancyInputs.add(borderCell);
    fancyInputs.addAll(CellData.forText(List.of(inputs)));
    
    var file = newPdfPath(label);
    
    report.writePdfFile(file, fancyInputs);
  }
  
  
  
  
  
  
  
  
  public static TableTemplate newTableTemplate(String resourceName) {
    var in = ReportTemplateTest.class.getResourceAsStream(resourceName);
    assertNotNull(in, resourceName);
    return TableTemplateParser.INSTANCE.toEntity(in);
  }
  
  
  
  public static ByteBuffer loadResourceBytes(String resourceName) throws IOException {
    var in = ReportTemplateTest.class.getResourceAsStream(resourceName);
    assertNotNull(in, resourceName);
    ByteArrayOutputStream collector = new ByteArrayOutputStream(8192);
    byte[] buffer = new byte[4096];
    for (int bytesRead = in.read(buffer); bytesRead != -1; bytesRead = in.read(buffer))
      collector.write(buffer, 0, bytesRead);
    return ByteBuffer.wrap(collector.toByteArray());
  }
  
  
  public static TreeMap<String, ByteBuffer> getRefedImages(String... resources) throws IOException {
    TreeMap<String, ByteBuffer> refedImages = new TreeMap<>();
    for (var resource : resources)
      refedImages.put(resource, loadResourceBytes(resource));
    return refedImages;
  }
  
  
  private Image loadImage(String resourceName) throws IOException {
    var url = getClass().getResource(resourceName);
    return Image.getInstance(url);
  }
  
  
  private File newPdfPath(Object label) {
    File dir = getMethodOutputFilepath(label);
    assertTrue( dir.mkdirs());
    return new File(dir, method(label) + ".pdf");
  }

}
