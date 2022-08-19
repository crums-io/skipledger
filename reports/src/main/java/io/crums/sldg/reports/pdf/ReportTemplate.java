/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.lowagie.text.Document;
import com.lowagie.text.PageSize;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfWriter;

import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.sldg.reports.pdf.input.Query;
import io.crums.sldg.src.SourceRow;

/**
 * 
 */
public class ReportTemplate {
  
  
  public record Components(
      Header header,
      Optional<TableTemplate> subHeader,
      TableTemplate mainTable,
      Optional<BorderContent> footer) {
    
    /**
     * The {@code header} and {@code mainTable} are required.
     * No null arguments.
     * 
     * @param header    (required)
     * @param subHeader (optional)
     * @param mainTable (required)
     * @param footer    (optional)
     * 
     * @throws NullPointerException if a required argument is {@code null}
     */
    public Components {
      Objects.requireNonNull(header, "null header");
      Objects.requireNonNull(subHeader, "null subHeader opt");
      Objects.requireNonNull(mainTable, "null main table");
      Objects.requireNonNull(footer, "null footer opt");
    }
    
    
    
    public Components(Header header,
      TableTemplate subHeader,
      TableTemplate mainTable,
      BorderContent footer) {
      this(header, Optional.ofNullable(subHeader), mainTable, Optional.ofNullable(footer));
    }
    
  }
  

  public final static float DEFAULT_MARGIN = 24;
  
  
  private final Components components;
  private final Query query;
  

//  private Rectangle pageSize = PageSize.A4;
  
  private float pageWidth = PageSize.A4.getWidth();
  
  private float pageHeight = PageSize.A4.getHeight();
  
  private float marginLeft = DEFAULT_MARGIN;
  
  private float marginRight = DEFAULT_MARGIN;
  
  private float marginTop = DEFAULT_MARGIN;
  
  private float marginBottom = DEFAULT_MARGIN;
  
  
  private Optional<String> description = Optional.empty();
  
  
  
  public ReportTemplate(Components components) {
    this(components, null);
  }
  
  public ReportTemplate(Components components, Query query) {
    this.components = Objects.requireNonNull(components, "null components");
    this.query = query;
  }
  
  /**
   * 
   * @param file
   * @param candidates  not-empty list of source rows. If there's a {@linkplain #getQuery() query}
   *                    object, then these are <em>candidate</em> source rows and are filtered by
   *                    the query object to create a final <em>rowset<em>
   * @throws IllegalArgumentException if the final rowset is empty
   */
  public void writePdf(File file, List<SourceRow> candidates)
      throws IllegalArgumentException, UncheckedIOException {
    
    Objects.requireNonNull(file, "null file");
    Objects.requireNonNull(candidates, "null candidate source rows");
    var rowset = query == null ? candidates : query.selectFrom(candidates);
    if (rowset.isEmpty())
      throw new IllegalArgumentException("empty rowset");
    if (file.exists())
      throw new IllegalArgumentException("cannot overwrite file: " + file);
    try {
      writePdfFile(file, rowset);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  protected void writePdfFile(File file, List<SourceRow> rowset) throws IOException {
    
    // create the PDF tables before writing anymore to the file
    // (avoid file I/O if an error is to occur)
    var headTable = components.header().headerTable().createTable(rowset);
    var subHeadTable = components.subHeader().map(subHeader -> subHeader.createTable(rowset));
    var mainTable = components.mainTable().createTable(rowset);

    ;
    var document = new Document(
        new Rectangle(pageWidth, pageHeight),
        marginLeft, marginRight, marginTop, marginBottom);
    
    // what's this? Can I add meta this way? TODO
//  document.addHeader("", "");
    
    PdfWriter.getInstance(
        document,
        new FileOutputStream(file));

    components.header().headContent().ifPresent(
        border -> document.setHeader(border.newHeaderFooter()));
    components.footer().ifPresent(
        footer -> document.setFooter(footer.newHeaderFooter()));
    
    // open file for writing
    document.open();
    document.add(headTable);
    subHeadTable.ifPresent(document::add);
    document.add(mainTable);
    document.close();
  }

  
  
  
  
  //         PROPERTY SETTERS / GETTERS
  
  
  public final Components getComponents() {
    return components;
  }
  
  
  /** @return may be {@code null} (!) */
  public Query getQuery() {
    return query;
  }
  
  /**
   * Returns the number args from the {@linkplain #getQuery() query}, if any.
   * 
   * @return not null, may be empty
   */
  public List<NumberArg> getNumberArgs() {
    return query == null ? List.of() : query.getNumberArgs();
  }
  
  
  /** @return {@code NumberArg.toArgMap(getNumberArgs())} */
  public Map<String, NumberArg> getNumberArgMap() {
    return NumberArg.toArgMap(getNumberArgs());
  }
  
  
  public List<NumberArg> getRequiredNumberArgs() {
    return getNumberArgs()
        .stream()
        .filter(NumberArg::isRequired)
        .toList();
  }
  
  
  /** @see NumberArg#bindValues(List, Map) */
  public void bindNumberArgs(Map<String, Number> input) throws IllegalArgumentException {
    NumberArg.bindValues(getNumberArgs(), input);
  }
  
  
  public final float getPageWidth() {
    return pageWidth;
  }
  
  
  public final float getPageHeight() {
    return pageHeight;
  }
  
  
  public void setPageSize(float width, float height) {
    if (width <= marginLeft + marginRight || height <= marginTop + marginBottom)
      throw new IllegalArgumentException("illegal page size (" + width + ":" + height + ")");
    this.pageWidth = width;
    this.pageHeight = height;
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
  
  
  public void setDescription(String description) {
    this.description =
        description == null || description.isBlank() ?
            Optional.empty() : Optional.of(description);
  }
  
  
  public final Optional<String> getDescription() {
    return description;
  }
  

  private void checkMargin(float margin) {
    if (margin < 0)
      throw new IllegalArgumentException("margin: " + margin);
  }
  
  
  @Override
  public final int hashCode() {
    int hash = components.hashCode();
    if (query != null)
      hash = hash * 31 + query.hashCode();
    int sizeHash = Float.hashCode(getPageWidth()) * 31 + Float.hashCode(getPageHeight());
    int marginHash = Float.hashCode(getMarginTop()) * 31 + Float.hashCode(getMarginLeft());
    return (hash * 7 + sizeHash) * 7 + marginHash;
  }
  

  @Override
  public final boolean equals(Object o) {
    return
        o instanceof ReportTemplate r &&
        r.components.equals(components) &&
        Objects.equals(r.query,  query) &&
        r.getPageWidth() == getPageWidth() &&
        r.getPageHeight() == getPageHeight() &&
        r.marginLeft == marginLeft &&
        r.marginRight == marginRight &&
        r.marginTop == marginTop &&
        r.marginBottom == marginBottom &&
        r.description.equals(description);
  }

}



















