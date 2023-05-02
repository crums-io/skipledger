/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.io.File;

/**
 * Exception indicating a hash conflict in a morsel file.
 * Thrown by {@linkplain StateMorselFiler}.
 */
@SuppressWarnings("serial")
public class MorselConflictException extends RowHashConflictException {
  
  
  private final File file;

  /**
   * Constructs an instance and generates a default message.
   * 
   * @param rn    the row number
   * @param file  the morsel's file
   */
  public MorselConflictException(long rn, File file) {
    this(rn, file,
        "row [%d] hash recorded in morsel %s conflicts with source"
        .formatted(rn, file.toString()));
  }

  /**
   * Full constructor.
   * 
   * @param rn      the row number
   * @param file    the morsel's file
   * @param message detail
   */
  public MorselConflictException(long rn, File file, String message) {
    super(rn, message);
    if (file == null)
      throw new RuntimeException("null file argument");
    this.file = file;
  }
  
  
  /** Returns the morsel's file. */
  public File getFile() {
    return file;
  }

}
