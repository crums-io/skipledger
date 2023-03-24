/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static io.crums.sldg.logs.text.LogledgeConstants.parseRnInName;
import static io.crums.sldg.logs.text.LogledgeConstants.rnFileComparator;
import static io.crums.sldg.logs.text.LogledgeConstants.rnFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.util.Lists;

/**
 * Utility for numbered files with uniform name prefix and extension.
 * The numbers are assumed to be non-negative.
 */
class NumberFiler {
  
  private final File dir;
  private final String prefix;
  private final String ext;
  
  

  /**
   * Constructor does not accept null arguments.
   * 
   * @param dir     path to a directory (does not have to necessarily exist)
   * @param prefix  filename prefix
   * @param ext     filename extension (include the dot '.')
   */
  NumberFiler(File dir, String prefix, String ext) {
    this.dir = dir;
    if (dir.isFile())
      throw new IllegalArgumentException("not a directory: " + dir);
    this.prefix = Objects.requireNonNull(prefix, "null prefix");
    this.ext = Objects.requireNonNull(ext, "null ext");
  }
  
  
  /** Returns the number inferred by the filename; -1 on failure to parse. */
  public long inferRn(File file) {
    return parseRnInName(file.getName(), prefix(), ext());
  }
  
  
  /** Returns the path associated with the given number. */
  public File rnFile(long rn) {
    return new File(dir(), rnFilename(rn));
  }
  
  
  /** Returns a file filter for the naming scheme with minimum number value. */
  public FileFilter newFileFilter(long minRn) {
    return rnFileFilter(prefix(), ext(), minRn);
  }
  
  /** Returns a file comparator based on the numbering scheme. Files
   * that fail the numbering scheme precede those properly named. */
  public Comparator<File> newFileComparator() {
    return rnFileComparator(prefix(), ext());
  }
  
  /**
   * Returns the filename for the given number. Overrides control.
   * 
   * @param rn  &ge; 0 (not checked)
   * 
   * @return {@code prefix() + rn + ext()}
   */
  public String rnFilename(long rn) {
    return prefix() + rn + ext();
  }
  
  
  /** Returns the [parent] directory files are placed in. Overrides control. */
  public File dir() {
    return dir;
  }
  
  /** Returns the filename prefix. Overrides control. */
  public String prefix() {
    return prefix;
  }

  /** Returns the filename extension. Overrides control. */
  public String ext() {
    return ext;
  }
  
  
  /**
   * Returns the highest numbered file, if present.
   */
  public Optional<File> lastFile() {
    var files = listFiles();
    return files.isEmpty() ?
        Optional.empty() :
          Optional.of(files.get(files.size() - 1));
  }
  
  
  /**
   * Lists the numbered files in the {@linkplain #dir() dir}.
   * 
   * @return {@code listFiles(true, 0)}
   */
  public List<File> listFiles() {
    return listFiles(true, 0);
  }
  
  
  /**
   * Lists the numbered files in the {@linkplain #dir() dir}.
   * 
   * @param sort    if {@code true}, then the list is sorted by number
   * @param minRn   min number
   * @return  not null
   */
  public List<File> listFiles(boolean sort, long minRn) {
    File[] files = dir().listFiles(newFileFilter(minRn));
    if (files == null)
      return List.of();
    if (sort && files.length > 1)
      Arrays.sort(files, newFileComparator());
    return Lists.asReadOnlyList(files);
  }
  

  
  /**
   * Lists the row numbers associated with the saved files in
   * ascending order.
   * 
   * @return {@code listFileRns(1L)}
   */
  public List<Long> listFileRns() {
    return listFileRns(1L);
  }
  
  /**
   * Lists the row numbers associated with the saved files in
   * ascending order.
   * 
   * @param minRn   min number
   */
  public List<Long> listFileRns(long minRn) {
    return Lists.map(listFiles(true, minRn), this::inferRn);
  }

}















