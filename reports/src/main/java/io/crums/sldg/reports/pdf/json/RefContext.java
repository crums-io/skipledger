/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.awt.Color;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.util.json.JsonParsingException;

/**
 * Object dictionaries for JSON parsing. This is a
 * de-duplication effort in JSON representation by introducing <em>referenced</em>
 * entities. It also doubles as an interface for loading raw image bytes (which I don't
 * want to encode in JSON).
 * 
 * <h3>Motivation</h3>
 * <p>
 * Many JSON snippets end up being duplicates of others. It would be nice to
 * avoid these duplicates both from a storage size standpoint, but more importantly
 * for the usual benefits of the DRY principle: it will allow one to craft <em>cleaner</em>,
 * maybe semantically more meaningful, templates.
 * </p>
 * <h3>Implementor's Reminder</h3>
 * <p>
 * The interface declaration inherits default implementations for every method. The
 * following are <em>controlling</em>:
 * <ul>
 * <li>{@linkplain #imageRefs()}</li>
 * <li>{@linkplain #colorRefs()}</li>
 * <li>{@linkplain #fontRefs()}</li>
 * <li>{@linkplain #cellFormatRefs()}</li>
 * <li>{@linkplain #cellDataRefs()}</li>
 * <li>{@linkplain #numberArgs()}</li>
 * </ul>
 * </p>
 * 
 * @see EditableRefContext
 * 
 */
public interface RefContext {
  
  
  /**
   * Stateless, read-only, empty instance.
   */
  public final static RefContext EMPTY = new RefContext() {  };
  
  
  public static RefContext ofImageRefs(Map<String, ByteBuffer> imageRefs) {
    Objects.requireNonNull(imageRefs, "null imageRefs");
    return new RefContext() {
      @Override
      public Map<String, ByteBuffer> imageRefs() {
        return imageRefs;
      }
    };
  }
  
  
  public static RefContext patchImageRefs(
      Map<String, ByteBuffer> imageRefs, RefContext context) {
    
    Objects.requireNonNull(imageRefs, "null imageRefs");
    if (context == null)
      context = EMPTY;
    return ofImpl(
        imageRefs,
        context.cellFormatRefs(),
        context.cellDataRefs(),
        context.colorRefs(),
        context.fontRefs(),
        context.numberArgs());
  }
  
  
  public static RefContext patchArgs(
      Map<String, NumberArg>  numberArgs, RefContext context) {
    
    Objects.requireNonNull(numberArgs, "null numberArgs");
    if (context == null)
      context = EMPTY;
    return ofImpl(
        context.imageRefs(),
        context.cellFormatRefs(),
        context.cellDataRefs(),
        context.colorRefs(),
        context.fontRefs(),
        numberArgs);
  }
  
  
  
  public static RefContext of(
      Map<String, ByteBuffer> imageRefs,
      Map<String, CellFormat> cellFormatRefs,
      Map<String, CellData>   cellDataRefs,
      Map<String, Color>      colorRefs,
      Map<String, FontSpec>   fontRefs,
      Map<String, NumberArg>  numberArgs) {

    imageRefs = transformNull(imageRefs);
    cellFormatRefs = transformNull(cellFormatRefs);
    cellDataRefs = transformNull(cellDataRefs);
    colorRefs = transformNull(colorRefs);
    fontRefs = transformNull(fontRefs);
    numberArgs = transformNull(numberArgs);

    return ofImpl(imageRefs, cellFormatRefs, cellDataRefs, colorRefs, fontRefs, numberArgs);
  }
  
  
  private static <T> Map<String, T> transformNull(Map<String, T> map) {
    return map == null ? Map.of() : map;
  }
  
  
  private static RefContext ofImpl(
      Map<String, ByteBuffer> imageRefs,
      Map<String, CellFormat> cellFormatRefs,
      Map<String, CellData>   cellDataRefs,
      Map<String, Color>      colorRefs,
      Map<String, FontSpec>   fontRefs,
      Map<String, NumberArg>  numberArgs) {

    return
        new RefContext() {
          @Override
          public Map<String, ByteBuffer> imageRefs() {
            return imageRefs;
          }
          @Override
          public Map<String, CellFormat> cellFormatRefs() {
            return cellFormatRefs;
          }
          @Override
          public Map<String, CellData> cellDataRefs() {
            return cellDataRefs;
          }
          @Override
          public Map<String, Color> colorRefs() {
            return colorRefs;
          }
          @Override
          public Map<String, FontSpec> fontRefs() {
            return fontRefs;
          }
          public Map<String, NumberArg> numberArgs() {
            return numberArgs;
          }
        };
  } // ofImpl(..
  
  
  private static <T> Optional<String> findRef(Map<String,T> map, T object) {
    return map.entrySet().stream().filter(e -> e.getValue().equals(object)).map(e -> e.getKey()).findAny();
  }
  
  
  private static <T> T getOrThrow(Map<String,T> map, String key, String failObject) {
    T value = map.get(key);
    if (value == null)
      throw new JsonParsingException("referenced " + failObject + " not found: " + key);
    return value;
  }
  
  
  
  
  
  default Map<String, ByteBuffer> imageRefs() {
    return Map.of();
  }
  
  default Map<String, CellFormat> cellFormatRefs() {
    return Map.of();
  }
  
  default Map<String, CellData> cellDataRefs() {
    return Map.of();
  }
  
  default Map<String, Color> colorRefs() {
    return Map.of();
  }
  
  default Map<String, FontSpec> fontRefs() {
    return Map.of();
  }
  
  default Map<String, NumberArg> numberArgs() {
    return Map.of();
  }
  
  
  /**
   * Returns the total number of referenced objects in this instance.
   */
  default int size() {
    return
        imageRefs().size() +
        sansImageSize();
  }
  
  
  /**
   * Returns the total number of referenced objects in this instance,
   * <em>excluding</em> the images.
   */
  default int sansImageSize() {
    return
        cellFormatRefs().size() +
        cellDataRefs().size() +
        colorRefs().size() +
        fontRefs().size() +
        numberArgs().size();
  }
  
  /**
   * Returns {@code true} iff there is no information in this instance.
   * 
   * @return {@code size() == 0}
   * @see #isEmptyJson()
   */
  default boolean isEmpty() {
    return size() == 0;
  }
  
  
  /**
   * Returns {@code true} iff there are no referenced JSON objects in this instance.
   * 
   * @return {@code sansImageSize() == 0}
   */
  default boolean isEmptyJson() {
    return sansImageSize() == 0;
  }
  

  // Ref look up methods (inefficient, but so what)
  
  
  default Optional<String> findRef(CellFormat format) {
    return findRef(cellFormatRefs(), format);
  }
  
  default Optional<String> findRef(CellData cellData) {
    return findRef(cellDataRefs(), cellData);
  }
  
  default Optional<String> findRef(Color color) {
    return findRef(colorRefs(), color);
  }
  
  default Optional<String> findRef(FontSpec font) {
    return findRef(fontRefs(), font);
  }
  
  
  
  
  
  /**
   * @throws JsonParsingException if not found
   */
  default ByteBuffer getBytes(String ref) throws JsonParsingException {
    return getOrThrow(imageRefs(), ref, "bytes");
  }

  /**
   * @throws JsonParsingException if not found
   */
  default CellFormat getCellFormat(String ref) throws JsonParsingException {
    return getOrThrow(cellFormatRefs(), ref, "cell format");
  }

  /**
   * @throws JsonParsingException if not found
   */
  default CellData getCellData(String ref) throws JsonParsingException {
    return getOrThrow(cellDataRefs(), ref, "cell data");
  }

  /**
   * @throws JsonParsingException if not found
   */
  default Color getColor(String ref) throws JsonParsingException {
    return getOrThrow(colorRefs(), ref, "color");
  }

  /**
   * @throws JsonParsingException if not found
   */
  default FontSpec getFont(String ref) throws JsonParsingException {
    return getOrThrow(fontRefs(), ref, "font");
  }
  /**
   * @throws JsonParsingException if not found
   */
  default NumberArg getNumberArg(String ref) throws JsonParsingException {
    return getOrThrow(numberArgs(), ref, "number arg");
  }

}













