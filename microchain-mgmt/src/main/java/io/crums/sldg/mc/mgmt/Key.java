/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;

import java.util.Comparator;

/**
 * Typed values used as keys in the database.
 * 
 * <h2>Purpose</h2>
 * <p>
 * This library does not use an external ORM framework/library: it
 * manages access to relational tables directly via its various "table-managers".
 * These tables are populated (and later read) according to a flow:
 * they often have foreign-primary/key relationships which dictate the
 * order in which information can be written or read, to and from tables.
 * So one "table-manager" is dependent on the work of another, and a primary
 * key value from one table can be a downstream argument to another
 * "table-manager".
 * </p><p>
 * The purpose of the {@linkplain IntKey} and {@linkplain LongKey} sub-types is
 * actually on the read-path. The only one who can instantiate a key sub-type
 * is the key's "table-manager". Any other table-manager that takes that
 * key as argument, does not need to verify the argument indeed comes from
 * the purported table.
 * </p>
 * <h2>Key Types</h2>
 * <p>
 * Presently, the primary keys in the database are one of 2 <em>integral</em>
 * SQL types: {@code INT} and {@code BIGINT}. These are encapsulated as
 * {@linkplain IntKey} and {@linkplain LongKey}, respectively.
 * </p>
 */
public class Key {
  
  private Key() { }
  
  /**
   * Integer-based key. Subclasses <em>must</em> override {@code Object.equals}.
   * 
   * @see #hashCode()
   */
  public static abstract class IntKey extends Key {
    
    public final static Comparator<IntKey> COMPARATOR = (a, b) -> a.no - b.no;
    
    private final int no;
    
    IntKey(int no) {
      this.no = no;
    }
    
    

    /** @return {@linkplain #no()} */
    @Override
    public final int hashCode() {
      return no;
    }
    
    /** Returns the key's number. */
    public final int no() {
      return no;
    }
    
    /** @return {@linkplain #no()} as a string. */
    @Override
    public String toString() {
      return Integer.toString(no);
    }
  }
  
  
  public static abstract class LongKey extends Key {
  
    public final static Comparator<LongKey> COMPARATOR =
        (a, b) -> Long.compare(a.no, b.no);
    
    private final long no;
    
    LongKey(long no) {
      this.no = no;
    }
    
    /** @return {@linkplain #no()} */
    public final int hashCode() {
      return Long.hashCode(no);
    }
    

    /** Returns the key's number. */
    public final long no() {
      return no;
    }
    
    
    public String toString() {
      return Long.toString(no);
    }
  }
  

}