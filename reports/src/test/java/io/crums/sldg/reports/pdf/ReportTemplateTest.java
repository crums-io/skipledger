/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.gnahraf.test.IoTestCase;
import com.lowagie.text.Font;
import com.lowagie.text.Image;

import io.crums.sldg.reports.pdf.Align.H;
import io.crums.sldg.reports.pdf.CellData.TextCell;
import io.crums.sldg.reports.pdf.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.StringProvider;
import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.reports.pdf.ReportTemplate.Components;
import io.crums.sldg.reports.pdf.SourcedCell.MultiStringCell;
import io.crums.sldg.reports.pdf.func.NumNode;
import io.crums.sldg.reports.pdf.func.BaseNumFunc;
import io.crums.sldg.reports.pdf.func.NumOp;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc.Column;
import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.sldg.reports.pdf.input.Query;
import io.crums.sldg.reports.pdf.pred.BoolComp;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.util.Lists;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class ReportTemplateTest extends IoTestCase {

  
  public final static String ICON = "example_icon.png";
  
  public final static String CHINOOK_MORSEL_263 = "chinook-263-262-261-260-.mrsl";
  
  public final static String CHINOOK_MORSEL_717 = "chinook-717-716-715-714-.mrsl";
  
  
  public final static int INVOICE_47 = 47;
  public final static int INVOICE_ID_COL_INDEX = 1;
  
  
  
  public static ReportTemplate queryWithTotalInstance() {
    
    try {
      var invoiceParam = new Param<Number>("invoice-id");
      var invoiceFunc = new SourceRowNumFunc.Supplied(
          List.of(new Column(INVOICE_ID_COL_INDEX)),
          null /* column value not manipulated */);
      var invoicePred = new SourceRowPredicate(
          invoiceFunc, BoolComp.EQ, new NumberArg(invoiceParam));
      
      return createWithTotal(new Query(invoicePred));
    
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  
  public static Map<String, ByteBuffer> iconMap() {
    try (var in = ReportTemplateTest.class.getResourceAsStream(ICON)) {
      var buffer  = BufferUtils.readFully(in);
      return Map.of(ICON, buffer);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  
  /** Contains invoice-47. */
  public static MorselPack getChinookMorsel() {
    return getMorsel(CHINOOK_MORSEL_263);
  }
  
  /** More src rows than invoice-47. */
  public static MorselPack getChinookMorsel717() {
    return getMorsel(CHINOOK_MORSEL_717);
  }
  
  public static MorselPack getMorsel(String resource) {
    try (var in = ReportTemplateTest.class.getResourceAsStream(resource)) {
      return  MorselPack.load(in);
    } catch (IOException iox) {
      fail(iox);
      return null;  // never reached
    }
  }

  
  
  @Test // <-- maven runs fine w/o the annotation; needed for IDE
  public void test00() throws Exception {
    var label = new Object() { };
    var icon = loadImage(ICON);

    CellData iconCell = CellData.forImage(ICON, icon, 50, 50);
    FontSpec font = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
    var cellFormat = new CellFormat(font);
    var protoHeadColumn = new ColumnTemplate(cellFormat, null);
    
    var headerTable = new TableTemplate(Lists.repeatedList(protoHeadColumn, 3));
    headerTable.setFixedCell(0,  1, iconCell);
    headerTable.setColumnWidths(List.of(0.25f, 0.5f, 0.25f));
    headerTable.setFixedCell(0, 2, CellData.TextCell.BLANK);
    
    var header = new Header(headerTable);
    
    var trackIdCell = new SourcedCell.StringCell(2, new StringProvider(), null);
    var unitPriceCell = new SourcedCell.NumberCell(3, new NumberProvider("###,###.##", "$"), null, null);
    var quantityCell = new SourcedCell.NumberCell(4, new NumberProvider(), null, null);
    
    ColumnTemplate[] columns = {
        new ColumnTemplate(cellFormat, trackIdCell),
        new ColumnTemplate(cellFormat, unitPriceCell),
        new ColumnTemplate(cellFormat, quantityCell)
    };
    
    var mainTable = new TableTemplate(List.of(columns));
    mainTable.setTableBorders(new LineSpec(3, Color.DARK_GRAY));
    
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Components(header, null, mainTable, footer));
    
    var file = newPdfPath(label);
    
    var sourceRows = getChinookMorsel().sources();
    report.writePdf(file, sourceRows);
  }
  
  
  @Test
  public void test00WithTrailingRows() throws Exception {
    var label = new Object() { };
    var icon = loadImage(ICON);

    CellData iconCell = CellData.forImage(ICON, icon, 50, 50);
    FontSpec font = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
    var cellFormat = new CellFormat(font);
    var protoHeadColumn = new ColumnTemplate(cellFormat, null);
    
    var headerTable = new TableTemplate(Lists.repeatedList(protoHeadColumn, 3));
    headerTable.setFixedCell(0,  1, iconCell);
    headerTable.setColumnWidths(List.of(0.25f, 0.5f, 0.25f));
    headerTable.setFixedCell(0, -2, CellData.TextCell.BLANK);   // <--- 2 rows after last
    
    var header = new Header(headerTable);
    
    var trackIdCell = new SourcedCell.StringCell(2, new StringProvider(), null);
    var unitPriceCell = new SourcedCell.NumberCell(3, new NumberProvider("###,###.##", "$"), null, null);
    var quantityCell = new SourcedCell.NumberCell(4, new NumberProvider(), null, null);
    
    ColumnTemplate[] columns = {
        new ColumnTemplate(cellFormat, trackIdCell),
        new ColumnTemplate(cellFormat, unitPriceCell),
        new ColumnTemplate(cellFormat, quantityCell)
    };
    
    var mainTable = new TableTemplate(List.of(columns));
    mainTable.setTableBorders(new LineSpec(3, Color.DARK_GRAY));
    
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Components(header, null, mainTable, footer));
    
    var file = newPdfPath(label);
    
    var sourceRows = getChinookMorsel().sources();
    report.writePdf(file, sourceRows);
  }
  
  
  @Test
  public void test00_Prettier() throws Exception {
    var label = new Object() { };
    var icon = loadImage(ICON);

    CellData iconCell = CellData.forImage(ICON, icon, 50, 50);
    FontSpec font = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
    var cellFormat = new CellFormat(font);
    cellFormat.setPadding(4);
    cellFormat.setLeading(8);
    var protoHeadColumn = new ColumnTemplate(cellFormat, null);
    
    var headerTable = new TableTemplate(Lists.repeatedList(protoHeadColumn, 3));
    headerTable.setFixedCell(0,  1, iconCell);
    headerTable.setColumnWidths(List.of(0.25f, 0.5f, 0.25f));
    headerTable.setFixedCell(0, 2, CellData.TextCell.BLANK);
    
    var header = new Header(headerTable);
    
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
    
    
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Components(header, null, mainTable, footer));
    
    var file = newPdfPath(label);
    
    var sourceRows = getChinookMorsel().sources();
    report.writePdf(file, sourceRows);
  }
  
  
  @Test
  public void testWithAddress() throws Exception {
    var label = new Object() { };
    var icon = loadImage(ICON);

    CellData iconCell = CellData.forImage(ICON, icon, 50, 50);
    FontSpec font = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
    var cellFormat = new CellFormat(font);
    cellFormat.setPadding(4);
    cellFormat.setLeading(8);
    var protoHeadColumn = new ColumnTemplate(cellFormat, null);
    
    var headerTable = new TableTemplate(Lists.repeatedList(protoHeadColumn, 3));
    {
      headerTable.setColumnWidths(List.of(0.25f, 0.5f, 0.25f));
      headerTable.setFixedCell(0,  1, iconCell);
      headerTable.setFixedCell(0, 2, CellData.TextCell.BLANK);
      
      var address = new SourcedCell.StringCell(7, new StringProvider(), null);
      var cityState = new MultiStringCell(List.of(8, 9), new StringProvider(), null);
      var countryPostal = new MultiStringCell(List.of(10, 11), new StringProvider(), null);
      
      headerTable.setFixedCell(0, 3, address);
      headerTable.setFixedCell(0, 4, cityState);
      headerTable.setFixedCell(0, 5, countryPostal);

      headerTable.setFixedCell(0, -1, CellData.TextCell.BLANK);
    }
    
    
    var header = new Header(headerTable);
    
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
    
    
    
    
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    var report = new ReportTemplate(new Components(header, null, mainTable, footer));
    
    var file = newPdfPath(label);
    
    var sourceRows = getChinookMorsel().sources();
    report.writePdf(file, sourceRows);
    
  }
  
  
  @Test
  public void testWithTotal() throws Exception {
    var label = new Object() { };
    var report = createWithTotal(null);
    var file = newPdfPath(label);
    
    var sourceRows = getChinookMorsel().sources();
    report.writePdf(file, sourceRows);
  }

  
  
  @Test
  public void testQueryTotal() throws Exception {

    var label = new Object() { };
    
    var report = queryWithTotalInstance();
    
    assertEquals(1, report.getNumberArgs().size());
    report.getNumberArgs().get(0).set(INVOICE_47);

    var file = newPdfPath(label);
    var sourceRows = getChinookMorsel717().sources();
    report.writePdf(file, sourceRows);
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private static ReportTemplate createWithTotal(Query query) throws IOException {
    
    var icon = loadImage(ICON);

    CellData iconCell = CellData.forImage(ICON, icon, 50, 50);
    FontSpec font = new FontSpec("Helvetica", 10, Font.NORMAL, Color.BLACK);
    var cellFormat = new CellFormat(font);
    cellFormat.setPadding(4);
    cellFormat.setLeading(8);
    var protoHeadColumn = new ColumnTemplate(cellFormat, null);
    
    var headerTable = new TableTemplate(Lists.repeatedList(protoHeadColumn, 3));
    {
      headerTable.setColumnWidths(List.of(0.25f, 0.5f, 0.25f));
      headerTable.setFixedCell(0,  1, iconCell);
      headerTable.setFixedCell(0, 2, CellData.TextCell.BLANK);
      
      var address = new SourcedCell.StringCell(7, new StringProvider(), null);
      var cityState = new MultiStringCell(List.of(8, 9), new StringProvider(), null);
      var countryPostal = new MultiStringCell(List.of(10, 11), new StringProvider(), null);
      
      headerTable.setFixedCell(0, 3, address);
      headerTable.setFixedCell(0, 4, cityState);
      headerTable.setFixedCell(0, 5, countryPostal);

      headerTable.setFixedCell(0, -1, CellData.TextCell.BLANK);
    }
    
    
    var header = new Header(headerTable);
    
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

    
    var emptyBlue = new TextCell(" ", headingStyle);
    mainTable.setFixedCell(0, -1, emptyBlue);
    mainTable.setFixedCell(1, -1, emptyBlue);
    mainTable.setFixedCell(2, -1, emptyBlue);
    
    
    BaseNumFunc columnsFunc;
    {
      var leaves = List.of(
          NumNode.newArgLeaf(),
          NumNode.newArgLeaf()
          );
      
      columnsFunc = new BaseNumFunc(NumNode.newBranch(NumOp.MULTIPLY, leaves));
    }
    
    var total = new SourcedCell.Sum(
        List.of(3,4), columnsFunc,
        new NumberProvider("###,###.##", "$"), null, null);

    mainTable.setFixedCell(1, -2, new TextCell("Total: "));
    mainTable.setFixedCell(2, -2, total);
    
    var footer = new BorderContent(
        "© 2020-2022 crums.io",
        new FontSpec("Helvetica", 8, Font.NORMAL, Color.GRAY), Align.H.RIGHT,
        false);
    
    return new ReportTemplate(new Components(header, null, mainTable, footer), query);
    
  }
  
  

  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private File newPdfPath(Object label) {
    File dir = getMethodOutputFilepath(label);
    assertTrue( dir.mkdirs());
    return new File(dir, method(label) + ".pdf");
  }
  

  
  
  private static Image loadImage(String resourceName) throws IOException {
    var url = ReportTemplateTest.class.getResource(resourceName);
    return Image.getInstance(url);
  }
  
  

}

















