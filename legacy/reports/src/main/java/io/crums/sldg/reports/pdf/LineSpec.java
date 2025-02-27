/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.awt.Color;
import java.util.Objects;

/**
 * Immutable line width and color specfication.
 */
public class LineSpec {
  

  /**
   * No line.
   */
  public final static LineSpec BLANK = new LineSpec(0, null);
  
  private final float width;
  private final Color color;
  
  
  /**
   * 
   * @param width &ge; 0
   * @param color {@code null} OK, but likely ineffective.
   */
  public LineSpec(float width, Color color) {
    if (width < 0)
      throw new IllegalArgumentException("width: " + width);
    this.width = width;
    this.color = color;
  }
  
  
  public final float getWidth() {
    return width;
  }
  
  public final boolean hasColor() {
    return color != null;
  }
  
  public final Color getColor() {
    return color;
  }
  
  public final boolean isBlank() {
    return width == 0;
  }
  
  
  /**
   * Instance equality depends on both width and color, unless width is zero.
   */
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof LineSpec) {
      var other = (LineSpec) o;
      if (width != other.width)
        return false;
      return width == 0 || Objects.equals(color, other.color) ;
    } else
      return false;
  }
  
  
  public final int hashCode() {
    int code = Float.hashCode(width);
    return color == null ? code : code ^ color.hashCode();
  }
  
}

  