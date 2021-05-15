/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.fs;


import java.io.File;
import java.io.IOException;

import io.crums.io.Opening;
import io.crums.sldg.CompactSkipLedger;
import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;

/**
 * A skip ledger stored directly in a file.
 */
public class SkipLedgerFile extends CompactSkipLedger {

  private final boolean lazy;
  
  
  


  
  /**
   * Creates a new or loads an existing ledger at the given {@code file} path with
   * the given {@linkplain Opening opening} {@code mode}. The instance is <em> not
   * lazy-loading</em>.
   * 
   * @param file path to a regular file
   * @param mode non-null
   */
  public SkipLedgerFile(File file, Opening mode) throws IOException {
    this(file, mode, false);
  }

  /**
   * Creates a new or loads an existing ledger at the given {@code file} path with
   * the given {@linkplain Opening opening} {@code mode}.
   * 
   * <p>By default, {@linkplain Row} instances <em>are not lazily loaded</em>,
   * i.e. they're pre-loaded, since if the row is to be serialized
   * externally anyway, it's better to fetch the data immediately while "disk" caches are still
   * hot. If the user however is just scanning rows, then the lazy version generally
   * performs better.
   * 
   * @param file path to a regular file
   * @param mode non-null
   * @param lazy if {@code true} then the row's hashpointers are lazily loaded ({@code false} by default)
   */
  public SkipLedgerFile(File file, Opening mode, boolean lazy) throws IOException {
    super(new SkipTableFile(file, mode));
    this.lazy = lazy;
  }

  
  @Override
  public Row getRow(long rowNumber) {
    Row row = super.getRow(rowNumber);
    return lazy ? row : new SerialRow(row);
  }

}











