/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import com.lowagie.text.Element;

/**
 * Horizontal and vertical alignment types.
 */
public class Align {
  
  
  public final static Align.H DEFAULT_H = H.LEFT;
  public final static Align.V DEFAULT_V = V.MIDDLE; 

  // only a namespace
  private Align() {  }
  
  /**
   * Horizontal alignment.
   */
  public enum H {
    LEFT(Element.ALIGN_LEFT),
    CENTER(Element.ALIGN_CENTER),
    RIGHT(Element.ALIGN_RIGHT),
    JUSTIFIED(Element.ALIGN_JUSTIFIED);
    
    private H(int code) {
      this.code = code;
    }
    
    public final int code;

    
    public boolean isDefault() {
      return this == DEFAULT_H;
    }
    
  }

  /**
   * Vertical alignment.
   */
  public enum V {
    BOTTOM(Element.ALIGN_BOTTOM),
    MIDDLE(Element.ALIGN_MIDDLE),
    TOP(Element.ALIGN_TOP);
    
    private V(int code) {
      this.code = code;
    }
    
    public final int code;
    
    
    public boolean isDefault() {
      return this == DEFAULT_V;
    }
    
  }

}
