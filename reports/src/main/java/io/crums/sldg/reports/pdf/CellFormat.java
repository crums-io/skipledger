/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;

import java.awt.Color;
import java.util.Objects;
import java.util.Optional;


/**
 * Specifies formatting and other presentation-related characteristics
 * of a PDF table cell. A first use is for specifying what the default
 * formatting for a column value should be; a second use is for overriding
 * those defaults on a cell-by-cell basis.
 * 
 * @see CellData
 */
public class CellFormat {
  
  

  
  private final FontSpec font;
  
  private float leading;
  private float padding;
  
  private Align.H alignH = Align.H.LEFT;
  private Align.V alignV = Align.V.MIDDLE;
  
  
  boolean ascender = true;
  boolean descender = true;
  
  private Color backgroundColor;
  
  
  
  public CellFormat(FontSpec font) {
    this.font = Objects.requireNonNull(font, "null font spec");
  }
  
  public CellFormat(CellFormat copy) {
    Objects.requireNonNull(copy, "null copy");
    this.font = copy.font;
    this.leading = copy.leading;
    this.padding = copy.padding;
    this.alignV = copy.alignV;
    this.alignH = copy.alignH;
    this.ascender = copy.ascender;
    this.descender = copy.descender;
    this.backgroundColor = copy.backgroundColor;
  }
  
  
  public final FontSpec getFont() {
    return font;
  }
  
  
  public final float getLeading() {
    return leading;
  }
  public void setLeading(float leading) {
    this.leading = leading;
  }
  
  
  public final float getPadding() {
    return padding;
  }
  public void setPadding(float padding) {
    this.padding = padding;
  }
  public void scalePaddingToFont(float scale) {
    if (font == null)
      throw new IllegalStateException("font not set");
    this.padding = font.getSize() * scale;
  }
  
  
  
  public final Align.H getAlignH() {
    return alignH;
  }
  public void setAlignH(Align.H alignH) {
    this.alignH = Objects.requireNonNull(alignH);
  }
  
  public final Align.V getAlignV() {
    return alignV;
  }
  public void setAlignV(Align.V alignV) {
    this.alignV = Objects.requireNonNull(alignV);
  }
  
  
  
  
  public final boolean isAscender() {
    return ascender;
  }
  public void setAscender(boolean ascender) {
    if (!ascender && !this.descender)
      throw new IllegalArgumentException("both ascender and descender cannot be 'false'");
    this.ascender = ascender;
  }
  
  
  public final boolean isDescender() {
    return descender;
  }
  public void setDescender(boolean descender) {
    if (!descender && !this.ascender)
      throw new IllegalArgumentException("both ascender and descender cannot be 'false'");
    this.descender = descender;
  }
  
  
  public final Optional<Color> getBackgroundColor() {
    return Optional.ofNullable(backgroundColor);
  }
  public void setBackgroundColor(Color color) {
    this.backgroundColor = color;
  }

  @Override
  public int hashCode() {
    return
        (font.hashCode() +
        alignH.hashCode() +
        alignV.hashCode() +
        Boolean.hashCode(ascender) -
        Boolean.hashCode(descender) +
        Float.hashCode(leading) +
        Float.hashCode(padding)) ^
        (backgroundColor == null ? -1 : backgroundColor.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    return
        obj instanceof CellFormat other &&
        other.font.equals(font) &&
        other.alignH == alignH &&
        other.alignV == alignV &&
        other.ascender == ascender &&
        other.descender == descender &&
        other.leading == leading &&
        other.padding == padding &&
        Objects.equals(other.backgroundColor, backgroundColor);
  }
  
  
  

}








