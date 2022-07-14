/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.lowagie.text.Document;
import com.lowagie.text.PageSize;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfWriter;

import io.crums.sldg.reports.pdf.CellData.TextCell;
import io.crums.sldg.reports.pdf.json.RefContext;

/**
 * 
 */
public class LegacyReportTemplate {
  
  
  /**
   * Constructor arguments lumped here.
   */
  public record Components(
      LegacyHeader header,
      LegacyTableTemplate subHeader,
      LegacyTableTemplate mainTable,
      BorderContent footer) {
    
    // constructors..
    
    /**
     * @param header    (required)
     * @param subHeader (optional)
     * @param mainTable (required)
     * @param footer    (optional)
     * 
     * @throws NullPointerException if a required argument is {@code null}
     */
    public Components {
      Objects.requireNonNull(header, "null header");
      Objects.requireNonNull(mainTable, "null main table");
    }
    
    /**
     * 
     * @param header
     * @param mainTable
     * @param footer
     */
    public Components(LegacyHeader header, LegacyTableTemplate mainTable, BorderContent footer) {
      this(header, null, mainTable, footer);
    }
    
  }
  
  
  public static boolean equal(LegacyReportTemplate a, LegacyReportTemplate b) {
    return
        a == b ||
        a != null && b != null &&
        a.marginLeft == b.marginLeft &&
        a.marginRight == b.marginRight &&
        a.marginTop == b.marginTop &&
        a.marginBottom == b.marginBottom &&
        a.header.equals(b.header) &&
        Objects.equals(a.footer, b.footer) &&
        LegacyTableTemplate.equal(a.mainTable, b.mainTable);
        
  }
  
  public final static float DEFAULT_MARGIN = 24;
  
  
  
  
  // BEGIN INSTANCE STUFF..
  
  
  private final LegacyHeader header;
  
  private final LegacyTableTemplate subHeader;
  private final LegacyTableTemplate mainTable;
  
  
  private final BorderContent footer;
  
  
  private RefContext references = RefContext.EMPTY;
  
  
  private Rectangle pageSize = PageSize.A4;
  
  private float marginLeft = DEFAULT_MARGIN;
  
  private float marginRight = DEFAULT_MARGIN;
  
  private float marginTop = DEFAULT_MARGIN;
  
  private float marginBottom = DEFAULT_MARGIN;
  
  
  
  
  /**
   * Currently equivalent to the full constructor.
   * 
   * @param header    non-null
   * @param mainTable non-null
   * @param footer    optional (may be null)
   * 
   * @see #ReportTemplate(Components)
   */
  public LegacyReportTemplate(
      LegacyHeader header, LegacyTableTemplate subHeader, LegacyTableTemplate mainTable, BorderContent footer) {
    this(new Components(header, subHeader, mainTable, footer));
  }
  

  /**
   * Currently equivalent to the full constructor.
   * 
   * @param header    non-null
   * @param mainTable non-null
   * @param footer    optional (may be null)
   * 
   * @see #ReportTemplate(Components)
   */
  public LegacyReportTemplate(LegacyHeader header, LegacyTableTemplate mainTable, BorderContent footer) {
    this(new Components(header, mainTable, footer));
  }
  
  
  /**
   * Main constructor takes a lumpy argument.
   */
  public LegacyReportTemplate(Components args) {
    this.header = args.header;
    this.subHeader = args.subHeader;
    this.mainTable = args.mainTable;
    this.footer = args.footer;
    
    header.getHeaderTable().setDefaultCell(TextCell.BLANK);
  }
  
  
  
  
  public final LegacyHeader getHeader() {
    return header;
  }
  
  
  /**
   * Returns the optional subheader table.
   */
  public final Optional<LegacyTableTemplate> getSubHeader() {
    return Optional.ofNullable(subHeader);
  }
  
  
  public final LegacyTableTemplate getMainTable() {
    return mainTable;
  }
  
  
  public final Optional<BorderContent> getFooter() {
    return Optional.ofNullable(footer);
  }
  
  
  
  /**
   * Write a PDF to the given {@code file} path.
   * 
   * @param file          path to new file (must not exist)
   * @param headCells
   * @param mainCells
   * @throws IOException
   */
  public void writePdfFile(
      File file, List<CellData> headCells, List<CellData> mainCells)
          throws IOException {

    Objects.requireNonNull(file, "null file");
    if (file.exists())
      throw new IllegalArgumentException(file + " already exists");
    if (!file.canWrite())
      throw new IllegalArgumentException("cannot write to " + file);
    
    final boolean headerCellsEmpty = headCells == null || headCells.isEmpty();
    if (subHeader == null) {
      if (!headerCellsEmpty)
        throw new IllegalArgumentException(
            "headCells must be empty when header table is not set: " + headCells);
    } else if (headerCellsEmpty)
      throw new IllegalArgumentException(
          "head cells empty while header table is set");
    Objects.requireNonNull(mainCells, "null mainCells");
    
    // done checking args..
    
    var headerPdfTable = header.getHeaderTable().createTable();
    var subHeaderPdfTable = subHeader == null ? null : subHeader.createTable(headCells);
    var mainPdfTable = mainTable.createTable(mainCells);
    
    var document = new Document(pageSize, marginLeft, marginRight, marginTop, marginBottom);
    PdfWriter.getInstance(
        document,
        new FileOutputStream(file));
    header.getHeadContent().ifPresent(border -> document.setHeader(border.newHeaderFooter()));
    if (footer != null)
      document.setFooter(footer.newHeaderFooter());
    
    
    document.open();
    document.add(headerPdfTable);
    if (subHeaderPdfTable != null)
      document.add(subHeaderPdfTable);
    document.add(mainPdfTable);
    document.close();
  }
  
  
  
  
  
  
  
  public void writePdfFile(File file, List<CellData> cells) throws IOException {
    Objects.requireNonNull(file, "null file");
    Objects.requireNonNull(cells, "null cells");
    List<CellData> headerCells, mainCells;
    {
      int headerCount = header.getHeaderTable().getDynamicCellCount();
      if (cells.size() < headerCount + mainTable.getColumnCount())
        throw new IllegalArgumentException("too few cells (" + cells.size() + ")");
      
      if (headerCount == 0) {
        headerCells = List.of();
        mainCells = cells;
      } else {
        headerCells = cells.subList(0, headerCount);
        mainCells = cells.subList(headerCount, cells.size());
      }
    }
    
    var headerPdfTable = header.getHeaderTable().createTable(headerCells);
    var mainPdfTable = mainTable.createTable(mainCells);
    
    var document = new Document(pageSize, marginLeft, marginRight, marginTop, marginBottom);
    PdfWriter.getInstance(
        document,
        new FileOutputStream(file));
    
    document.addHeader("", "");
    
    header.getHeadContent().ifPresent(border -> document.setHeader(border.newHeaderFooter()));
    if (footer != null)
      document.setFooter(footer.newHeaderFooter());
    
    
    document.open();
    document.add(headerPdfTable);
    document.add(mainPdfTable);
    document.close();
  }
  
  
  public final RefContext getReferences() {
    return references;
  }
  
  
  public void setReferences(RefContext references) {
    this.references = references == null ? RefContext.EMPTY : references;
  }
  
  
  public final float getPageWidth() {
    return pageSize.getWidth();
  }
  
  
  public final float getPageHeight() {
    return pageSize.getHeight();
  }
  
  
  public void setPageSize(float width, float height) {
    if (width <= marginLeft + marginRight || height <= marginTop + marginBottom)
      throw new IllegalArgumentException("illegal page size (" + width + ":" + height + ")");
    this.pageSize = new Rectangle(width, height);
  }


  public final float getMarginLeft() {
    return marginLeft;
  }


  public final float getMarginRight() {
    return marginRight;
  }


  public final float getMarginTop() {
    return marginTop;
  }


  public final float getMarginBottom() {
    return marginBottom;
  }
  
  
  public final boolean sameMargins() {
    return
        marginLeft == marginRight &&
        marginLeft == marginTop &&
        marginLeft == marginBottom;
  }
  
  
  public void setMargins(float margin) {
    setMarginLeft(margin);
    setMarginRight(margin);
    setMarginTop(margin);
    setMarginBottom(margin);
  }


  public void setMarginLeft(float margin) {
    checkMargin(margin);
    this.marginLeft = margin;
  }


  public void setMarginRight(float margin) {
    checkMargin(margin);
    this.marginRight = margin;
  }


  public void setMarginTop(float margin) {
    checkMargin(margin);
    this.marginTop = margin;
  }


  public void setMarginBottom(float margin) {
    checkMargin(margin);
    this.marginBottom = margin;
  }
  

  private void checkMargin(float margin) {
    if (margin < 0)
      throw new IllegalArgumentException("margin: " + margin);
  }
  
}
