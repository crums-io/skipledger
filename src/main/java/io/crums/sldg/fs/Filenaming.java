/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.fs;


import static io.crums.sldg.SldgConstants.JSON_EXT;
import static io.crums.sldg.SldgConstants.SPATH_EXT;
import static io.crums.sldg.SldgConstants.SPATH_JSON_EXT;

import java.io.File;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.Path;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.src.SourceRow;
import io.crums.util.IntegralStrings;

/**
 * The file naming convention.
 */
public class Filenaming {
  
  
  public final static Filenaming INSTANCE = new Filenaming();

  /**
   * Number of bytes displayed in hex in a path-like filename.
   */
  public final static int ENTRY_BYTES_DISPLAY = 3;
  
  /**
   * Token separator used in generated filenames.
   */
  public final static String SEP = "-";
  
  
  public final static String STATE = "state";
  

  private final static int MAX_ECOUNT_IN_NAME = 4;
  
  
  
  public String morselFilename(String name, MorselBag bag) {
    Objects.requireNonNull(name, "null name");
    Objects.requireNonNull(bag, "null builder");
    
    if (name == null || name.isEmpty() || !name.equals(name.trim()))
        throw new IllegalArgumentException("name '" + name + "'");

    List<SourceRow> srcs = bag.sources();
    String filename = name;
    if (srcs.isEmpty()) {
      filename += "-state-" + bag.hi();
    } else {
      for (int index = srcs.size(), count = MAX_ECOUNT_IN_NAME; index-- > 0 && count-- > 0; )
        filename += "-" + srcs.get(index).rowNumber();
      
      if (srcs.size() > MAX_ECOUNT_IN_NAME)
        filename += "-";
    }
    
    filename += SldgConstants.MRSL_EXT;
    return filename;
  }
  
  
  
  /**
   * Generates and returns a filename for the given <tt>path</tt>.
   */
  public String pathFilename(Path path, Format format) {
    Objects.requireNonNull(path, "null path");
    Objects.requireNonNull(format, "null format");
    
    String name = makePrefix(path);
    
    name += SPATH_EXT;
    if (format.isJson())
      name += SldgConstants.JSON_EXT;
    
    return name;
  }
  
  
  /**
   * Generates and returns a filename for the given state path.
   */
  public String stateFilename(Path path, Format format) {
    Objects.requireNonNull(path, "null path");
    Objects.requireNonNull(format, "null format");
    
    String name =
        STATE + SEP + path.hiRowNumber() + SEP +
        IntegralStrings.toHex(path.last().hash().limit(ENTRY_BYTES_DISPLAY)) +
        SPATH_EXT;

    if (format.isJson())
      name += SldgConstants.JSON_EXT;
    
    return name;
  }
  
  
  
  public Format guessFormat(File file) {
    return
        Objects.requireNonNull(file, "null file").getName().endsWith(JSON_EXT) ?
            Format.JSON : Format.BINARY;
  }
  
  
  
  public boolean isPath(File file) {
    String name = Objects.requireNonNull(file, "null file").getName();
    return name.endsWith(SPATH_EXT) || name.endsWith(SPATH_JSON_EXT);
  }
  
  
  
  
  
  private String makePrefix(Path path) {
    return
        path.loRowNumber() + SEP +
        IntegralStrings.toHex(path.first().inputHash().limit(ENTRY_BYTES_DISPLAY)) + SEP +
        path.hiRowNumber();
  }
  
  
  
  /**
   * Returns a normalized version of the <tt>input</tt> filename in the given
   * <tt>format</tt> for a {@linkplain Path path}. It's to make user-input reasonable.
   */
  public String normalizePathFilename(String input, Format format) {
    return normalizeEnityFilename(input, format, SPATH_EXT);
  }
  
  
  private String normalizeEnityFilename(String input, Format format, String binExt) {
    Objects.requireNonNull(input, "null input");
    Objects.requireNonNull(format, "null format");
    
    
    
    switch (format) {
    
    case BINARY:
      if (input.endsWith(JSON_EXT))
        throw new IllegalArgumentException(
            "cannot normalize misleading extension in '" + input + "' for " + format);
      return input.endsWith(binExt) ? input : input + binExt;
    
    case JSON:
      final String outExt = binExt + JSON_EXT;
      if (input.endsWith(outExt))
        return input;
      while (input.endsWith(binExt) || input.endsWith(JSON_EXT)) {
        int newLength = input.length();
        if (input.endsWith(binExt))
          newLength -= binExt.length();
        else
          newLength -= JSON_EXT.length();
        
        input = input.substring(0, newLength);
      }
      return input + outExt;
      
    default:
      throw new RuntimeException("unaccounted format " + format);
    }
    
  }

}
