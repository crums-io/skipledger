/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.io.File;
import java.io.IOException;

import io.crums.io.Opening;
import io.crums.sldg.db.CompactFileLedger;
import io.crums.sldg.db.VolatileTable;

/**
 * Convenience methods for creating or loading skip ledgers.
 */
public class Ledgers {
  
  // no one calls
  private Ledgers() {  }
  

  
  /**
   * 
   * Creates a new or loads an existing ledger at the given <tt>file</tt> path.
   * 
   * @param file path to a regular file
   */
  public static Ledger newLedger(File file) throws IOException {
    return newLedger(file, Opening.CREATE_ON_DEMAND);
  }
  
  /**
   * Creates a new or loads an existing ledger at the given <tt>file</tt> path with
   * the given {@linkplain Opening opening} <tt>mode</tt>.
   * 
   * @param file path to a regular file
   * @param mode non-null
   */
  public static Ledger newLedger(File file, Opening mode) throws IOException {
    return newLedger(file, mode, false);
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
   * @param lazy if <tt>true</tt> then the row's hashpointers are lazily loaded
   *        (<tt>false</tt> by default)
   */
  public static Ledger newLedger(File file, Opening mode, boolean lazy) throws IOException {
    return new CompactFileLedger(file, mode, lazy);
  }
  
  
  /**
   * Creates and returns an in-memory, volatile ledger. This may find other uses; for now
   * it's used mostly in testing.
   */
  public static Ledger newVolatileLedger() {
    return new CompactLedger(new VolatileTable());
  }

}
