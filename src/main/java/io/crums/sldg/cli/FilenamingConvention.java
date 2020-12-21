/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.sldg.SldgConstants.JSON_EXT;
import static io.crums.sldg.SldgConstants.NUG_EXT;
import static io.crums.sldg.SldgConstants.SPATH_EXT;

import java.util.Objects;

import io.crums.sldg.Nugget;
import io.crums.sldg.Path;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.db.Format;
import io.crums.util.IntegralStrings;

/**
 * The file naming convention.
 */
public class FilenamingConvention {
  
  
  public final static FilenamingConvention INSTANCE = new FilenamingConvention();

  /**
   * Number of bytes displayed in hex in a path-like filename.
   */
  public final static int ENTRY_BYTES_DISPLAY = 3;
  
  /**
   * Token separator used in generated filenames.
   */
  public final static String SEP = "-";
  
  
  public final static String STATE = "state";
  
  
  
  

  /**
   * Generates and returns a filename for the given <tt>nugget</tt>.
   */
  public String nuggetFilename(Nugget nugget, Format format) {
    Objects.requireNonNull(nugget, "null nugget");
    Objects.requireNonNull(format, "null format");
    
    String name = makePrefix(nugget.ledgerPath());
    
    name += SldgConstants.NUG_EXT;
    if (format.isJson())
      name += SldgConstants.JSON_EXT;
    return name;
  }
  
  
  
  /**
   * Generates and returns a filename for the given <tt>path</tt>.
   */
  public String pathFilename(Path path, Format format) {
    Objects.requireNonNull(path, "path nugget");
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
    Objects.requireNonNull(path, "path nugget");
    Objects.requireNonNull(format, "null format");
    
    String name =
        STATE + SEP + path.hiRowNumber() + SEP +
        IntegralStrings.toHex(path.last().hash().limit(ENTRY_BYTES_DISPLAY)) +
        SPATH_EXT;

    if (format.isJson())
      name += SldgConstants.JSON_EXT;
    
    return name;
  }
  
  
  
  
  
  private String makePrefix(Path path) {
    return
        path.loRowNumber() + SEP +
        IntegralStrings.toHex(path.first().inputHash().limit(ENTRY_BYTES_DISPLAY)) + SEP +
        path.hiRowNumber();
  }
  
  
  /**
   * Returns a normalized version of the <tt>input</tt> filename in the given
   * <tt>format</tt> for a {@linkplain Nugget nugget}. It's to make user-input reasonable.
   */
  public String normalizeNuggetFilename(String input, Format format) {
    return normalizeEnityFilename(input, format, NUG_EXT);
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
