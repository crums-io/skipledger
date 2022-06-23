/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.awt.Color;
import java.util.Objects;

import com.lowagie.text.Font;
import com.lowagie.text.FontFactory;

/**
 * Created because there's no easy way to recover {@code FontFactory} arguments from
 * {@code Font} objects. Instances are immutable.
 */
public class FontSpec {
  
  private final String name;
  private final float size;
  private final int style;
  private final Color color;
  
  private volatile Font font;
  
  
  public FontSpec(String name, float size, int style, Color color) {
    this.name = Objects.requireNonNull(name, "null name");
    this.size = size;
    this.style = style;
    this.color = color == null ? Color.BLACK : color;
  }


  public final String getName() {
    return name;
  }


  public final float getSize() {
    return size;
  }


  public final int getStyle() {
    return style;
  }


  public final Color getColor() {
    return color;
  }


  public Font getFont() {
    if (font == null)
      font = FontFactory.getFont(name, size, style, color);
    return font;
  }
  
  
  @Override
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof FontSpec) {
      var other = (FontSpec) o;
      return
          other.name.equals(name) &&
          other.size == size &&
          other.style == style &&
          other.color.equals(color);
    } else
      return false;
  }
  

  @Override
  public final int hashCode() {
    return name.hashCode() ^ Float.hashCode(size) ^ style ^ color.hashCode();
  }

}
