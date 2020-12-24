/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;


import java.io.File;
import java.io.IOException;

import io.crums.io.Opening;
import io.crums.sldg.CompactLedger;
import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;

/**
 * A ledger with backing storage on the file system.
 */
public class CompactFileLedger extends CompactLedger {
  
  
  private final boolean lazy;
  
  
  /**
   * Creates a new or loads an existing ledger at the given <tt>file</tt> path.
   * 
   * @param file path to a regular file
   */
  public CompactFileLedger(File file) throws IOException {
    this(file, Opening.CREATE_ON_DEMAND);
  }
  
  /**
   * Creates a new or loads an existing ledger at the given <tt>file</tt> path with
   * the given {@linkplain Opening opening} <tt>mode</tt>.
   * 
   * @param file path to a regular file
   * @param mode non-null
   */
  public CompactFileLedger(File file, Opening mode) throws IOException {
    this(file, mode, false);
  }
  
  
  /**
   * Creates a new or loads an existing ledger at the given <tt>file</tt> path with
   * the given {@linkplain Opening opening} <tt>mode</tt>.
   * 
   * <p>By default, {@linkplain Row} instances <em>are not</em> lazily loaded,
   * i.e. they're pre-loaded, since if the row is to be serialized
   * externally anyway, it's better to fetch the data immediately while "disk" caches are still
   * hot. If the user however is just scanning rows, then the lazy version generally
   * performs better.
   * 
   * @param file path to a regular file
   * @param mode non-null
   * @param lazy if <tt>true</tt> then the row's hashpointers are lazily loaded (<tt>false</tt> by default)
   * @throws IOException
   */
  public CompactFileLedger(File file, Opening mode, boolean lazy) throws IOException {
    super(new FileTable(file, mode));
    this.lazy = lazy;
  }

  
  @Override
  public Row getRow(long rowNumber) {
    Row row = super.getRow(rowNumber);
    return lazy ? row : new SerialRow(row);
  }
  
  

}
