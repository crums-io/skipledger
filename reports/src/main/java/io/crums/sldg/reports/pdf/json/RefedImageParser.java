/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;


/**
 * Deduplication base-class for JSON parsers that access externally defined
 * named bytes. The use case here is for raw image bytes.
 */
public abstract class RefedImageParser<T> implements ContextedParser<T> {

  protected final Map<String, ByteBuffer> refedImages;
  
  
  /**
   * Creates an empty instance, unmodifiable instance.
   */
  protected RefedImageParser() {
    this.refedImages = Map.of();
  }
  
  /**
   * Creates an instance with the given named images. The argument is not
   * copied.
   * 
   * @param refedImages non-null, but empty OK
   */
  protected RefedImageParser(Map<String, ByteBuffer> refedImages) {
    this.refedImages = Objects.requireNonNull(refedImages, "null refedImages");
  }

}
