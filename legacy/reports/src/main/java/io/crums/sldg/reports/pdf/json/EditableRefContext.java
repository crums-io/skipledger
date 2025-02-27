/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.awt.Color;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.util.Maps;

/**
 * The typed maps returned by this implementation are editable and sorted.
 */
public class EditableRefContext implements RefContext {
  
  
  private final Map<String, ByteBuffer> imageRefs;
  private final TreeMap<String, Color> colorRefs = new TreeMap<>();
  private final TreeMap<String, FontSpec> fontRefs = new TreeMap<>();
  private final TreeMap<String, CellFormat> cellFormatRefs = new TreeMap<>();
  private final TreeMap<String, CellData> cellDataRefs = new TreeMap<>();
  private final TreeMap<String, NumberArg> numberArgs = new TreeMap<>();
  
  
  public EditableRefContext() {
    this.imageRefs = new TreeMap<>();
  }
  
  public EditableRefContext(Map<String, ByteBuffer> imageRefs) {
    this.imageRefs = Objects.requireNonNull(imageRefs, "null imageRefs");
  }
  
  public EditableRefContext(RefContext copy) {
    this();
    imageRefs.putAll(copy.imageRefs());
    colorRefs.putAll(copy.colorRefs());
    fontRefs.putAll(copy.fontRefs());
    cellFormatRefs.putAll(copy.cellFormatRefs());
    cellDataRefs.putAll(copy.cellDataRefs());
    numberArgs.putAll(copy.numberArgs());
  }

  
  
  
  @Override
  public Map<String, ByteBuffer> imageRefs() {
    return Maps.mapValues(imageRefs, ByteBuffer::slice);
  }

  @Override
  public SortedMap<String, CellFormat> cellFormatRefs() {
    return cellFormatRefs;
  }

  @Override
  public SortedMap<String, CellData> cellDataRefs() {
    return cellDataRefs;
  }

  @Override
  public SortedMap<String, Color> colorRefs() {
    return colorRefs;
  }

  @Override
  public SortedMap<String, FontSpec> fontRefs() {
    return fontRefs;
  }
  
  
  @Override
  public SortedMap<String, NumberArg> numberArgs() {
    return numberArgs;
  }
  
  
  
  
  /**
   * Maps the number arg's paramater name to the given number arg and
   * returns whether the invocation had any result. (Recall a number
   * arg's equality semantics is solely based on it's parameter name.)
   * 
   * @return {@code true} if there wasn't a previous mapping.
   */
  public boolean putNumberArg(NumberArg arg) {
    return numberArgs.put(arg.param().name(), arg) == null;
  }
  
  
  /** @return {@code putNumberArg(new NumberArg(param))} */
  public boolean putNumberArg(Param<Number> param) {
    return putNumberArg(new NumberArg(param));
  }

}
