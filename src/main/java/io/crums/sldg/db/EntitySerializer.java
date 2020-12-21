/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;

import java.io.File;
import java.io.UncheckedIOException;
import java.util.Objects;


/**
 * Interface for loading an entity (an object of type <tt>T</tt>) from a standalone file.
 * The methods specified here take only non-null arguments and never return null. I/O
 * errors are raised by {@linkplain UncheckedIOException}s.
 * 
 * @param <T> the entity type
 */
public interface EntitySerializer<T> {
  
  
  /**
   * Loads and returns the entity from the file by guessing its {@linkplain Format format} from its filename
   * extension. The current convention is that absent a recognized extension,
   * a {@linkplain Format#BINARY binary} format is assumed. The only recognized file extension
   * is currently <em><tt>.json</tt></em>.
   * 
   * @param file single-entity file
   */
  default T load(File file) {
    Objects.requireNonNull(file, "null file");
    int extIndex = file.getName().lastIndexOf('.');
    boolean json =
        extIndex != -1 && ".json".equalsIgnoreCase(file.getName().substring(extIndex));
    return json ? loadJson(file) : loadBinary(file);
        
  }
  
  /**
   * Loads and returns the entity from the given file. This is probably just a pedantic
   * method for closure: doubt it'll be used it much.
   * 
   * @param format the file format
   */
  default T load(File file, Format format) {
    Objects.requireNonNull(file, "null file");
    Objects.requireNonNull(format, "null format");
    if (!file.isFile())
      throw new IllegalArgumentException("not a file: " + file);
    
    switch (format) {
    case BINARY:
      return loadBinary(file);
    case JSON:
      return loadJson(file);
    default:
      throw new UnsupportedOperationException("no " + format + " handler present");
    }
  }
  
  
  /**
   * Loads the given JSON file and returns the entity in it.
   * 
   * @param file JSON file
   */
  T loadJson(File file);
  
  /**
   * Loads the given binary file and return the entity in it.
   * 
   * @param file binary file
   */
  T loadBinary(File file);
  
  
  /**
   * Writes the given <tt>entity</tt> as JSON to the specified <tt>file</tt>.
   */
  void writeJson(T entity, File file);
  

  /**
   * Writes the given <tt>entity</tt> in binary format to the specified <tt>file</tt>.
   */
  void writeBinary(T entity, File file);
  
  
  /**
   * Writes an <tt>entity</tt> to the specified <tt>file</tt> in the given <tt>format</tt>.
   */
  default void write(T entity, File file, Format format) {
    switch (format) {
    case BINARY:
      writeBinary(entity, file);
      break;
    case JSON:
      writeJson(entity, file);
      break;
    default:
      throw new UnsupportedOperationException("no " + format + " handler present");
    }
  }

}
