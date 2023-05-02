/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.File;
import java.io.FileFilter;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

import io.crums.sldg.ByteFormatException;
import io.crums.sldg.SldgConstants;
import io.crums.util.Strings;

/**
 * 
 */
public class LogledgeConstants {

  // never
  private LogledgeConstants() {  }
  
  
  public final static String LOGNAME = "sldg.logs";
  
  public static Logger sysLogger() {
    return System.getLogger(LOGNAME);
  }
  

  public final static int UNUSED_PAD = 2;

  
  public final static String PREFIX = ".sldg.";
  

  public final static String SEAL_EXT = ".seal";
  public final static String PSEAL_EXT = ".pseal";
  
  

  /** Filename prefix used in numbered frontier state files. */
  public final static String STATE_PREFIX = "_";
  /** Filename extension for numbered frontier state files. Each records a {@linkplain Fro}. */
  public final static String STATE_EXT = ".fstate";
  
  /**
   * Filename prefix used in numbered state morsel files.
   * @see #STATE_MRSL_EXT
   */
  public final static String STATE_MRSL_PREFIX = "state-";
  /** Filename extension for numbered state morsel files. */
  public final static String STATE_MRSL_EXT = SldgConstants.MRSL_EXT;
  
  /** Frontier hash filename. */
  public final static String FRONTIERS_FILE = "fhash";
  /** EOR (end-of-row/line) offsets filename. */
  public final static String OFFSETS_FILE = "eor";



  public final static String SEAL_MAGIC = "seal";
  public final static String FRONTIERS_MAGIC = FRONTIERS_FILE;
  public final static String OFFSETS_MAGIC = OFFSETS_FILE;
  public final static String STATE_MAGIC = "fstate";
  
  
  /**
   * Returns the magic header length in bytes. This includes the unused padding
   * {@linkplain #UNUSED_PAD} bytes.
   * 
   * @param magic   ASCII magic string
   * @return {@code magic.length() + UNUSED_PAD}
   */
  public static int magicHeaderLen(String magic) {
    return magic.length() + UNUSED_PAD;
  }
  
  /**
   * Advances the given buffer beyond the magic header. This includes the unused padding
   * {@linkplain #UNUSED_PAD} bytes.
   * 
   * @param magic   ASCII magic string
   * 
   * @throws ByteFormatException if the {@code magic} bytes are not found
   */
  public static void assertMagicHeader(String magic, ByteBuffer buffer) throws ByteFormatException {
    for (int index = 0; index < magic.length(); ++index) {
      if (buffer.get() != magic.charAt(index))
        throw new ByteFormatException(
            "magic bytes '%s' not matched".formatted(magic));
    }
    buffer.position(buffer.position() + UNUSED_PAD);
  }
  
  
  /**
   * Writes the magic header. This includes the unused padding
   * {@linkplain #UNUSED_PAD} bytes.
   * 
   * @param magic   ASCII magic string
   * @param buffer  usually positioned at zero (not checked)
   */
  public static void writeMagicHeader(String magic, ByteBuffer buffer) {
    buffer.put(magic.getBytes(Strings.UTF_8));
    for (int pad = UNUSED_PAD; pad-- > 0;)
      buffer.put((byte) 0);
  }
  
  
  public static String sealFilename(File log) {
    return PREFIX + log.getName() + SEAL_EXT;
  }
  
  public static String pendingSealFilename(File log) {
    return PREFIX + log.getName() + PSEAL_EXT;
  }
  
  
  public static File sealFile(File log) {
    return new File(log.getParentFile(), sealFilename(log));
  }
  
  
  public static File pendingSealFile(File log) {
    return new File(log.getParentFile(), pendingSealFilename(log));
  }
  
  /**
   * Parses the given {@code name} and returns the decimal number sandwiched
   * between in the {@code prefix} and {@code suffix}, or {@code -1L}, if not
   * found.
   */
  public static long parseRnInName(String name, String prefix, String suffix) {
    if (!name.startsWith(prefix) || !name.endsWith(suffix))
      return -1L;
    var rn = name.substring(prefix.length(), name.length() - suffix.length());
    try {
      return Long.parseLong(rn);
    } catch (Exception ignore) {
      sysLogger().log(Level.TRACE, "failed parsing no. in funky name: " + name);
      return -1L;
    }
  }
  
  
  public static FileFilter rnFileFilter(String prefix, String ext, long min) {
    if (min < 0)
      throw new IllegalArgumentException("min %d < 0".formatted(min));
    Objects.requireNonNull(prefix, "null prefix");
    Objects.requireNonNull(ext, "null ext");
    return (f) -> parseRnInName(f.getName(), prefix, ext) >= min;
  }
  
  
  public static Comparator<File> rnFileComparator(String prefix, String ext) {
    Objects.requireNonNull(prefix, "null prefix");
    Objects.requireNonNull(ext, "null ext");
    return new Comparator<File>() {
      @Override
      public int compare(File a, File b) {
        long an = parseRnInName(a.getName(), prefix, ext);
        long bn = parseRnInName(b.getName(), prefix, ext);
        return an < bn ? -1 : (an == bn ? 0 : 1);
      }
    };
  }
  
}
