/*
 * Copyright 2020-2024 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.List;

import io.crums.util.Lists;

/**
 * A row in a ledger. Concrete instances must have immutable state.
 * The fields of an instance must satisfy the following hash-relation:
 * <p>
 *  {@code hash().equals(SkipLedger.rowHash(inputHash(), levelsPointer().hash()))}
 * </p><p>
 * Tho all subclasses defined in this module enforce this rule, the types
 * defined in this module are not sealed, so it's conceivable a subclass may
 * violate the constraint.
 * </p>
 * <h2>Overridable / Abstract Methods</h2>
 * <p>
 * All but 3 methods in this class are marked <b>final</b>. 2 of those 3
 * {@link #inputHash()} and {@link #levelsPointer()} are abstract, and
 * together they drive the return values for the other methods. The only
 * non-final method is {@link #hash()} which generates the RHS of the
 * hash-relation mentioned above. The reason why it's not made final is
 * that an implementation might have memo-ized the result of the hashing.
 * </p>
 * 
 * @see #inputHash()
 * @see #levelsPointer()
 * @see #hash()
 * 
 * @see RowHash#prevLevels()
 * @see RowHash#prevNo(int)
 */
public abstract class Row extends RowHash {
  
  
  /**
   * {@inheritDoc}
   * 
   * @return {@code levelsPointer().rowNo()}
   */
  @Override
  public final long no() {
    return levelsPointer().rowNo();
  }
  

  /**
   * Returns the user-inputed hash. This is the hash of the abstract object
   * the entered the ledger (whatever it is, we don't know).
   * 
   * @return non-null, 32-bytes remaining
   */
  public abstract ByteBuffer inputHash();
  


  /**
   * Returns the levels pointer.
   */
  public abstract LevelsPointer levelsPointer();

  
  
  /**
   * {@inheritDoc}
   * The default implementation derives (calculates) the hash value using
   * the {@link #inputHash()} and the {@link #levelsPointer()}.
   * 
   * @return {@code SkipLedger.rowHash(inputHash(), levelsPointer().hash())}
   */
  @Override
  public ByteBuffer hash() {
    return SkipLedger.rowHash(inputHash(), levelsPointer().hash());
  }


  /** Valid only if <em>not</em> condensed. */
  final List<ByteBuffer> levelHashes() {
    return Lists.functorList(
        prevLevels(),
        this::prevHash);
  }




  /**
   * Tests whether the instance's hash is computed using
   * condensed level row hashes.
   * 
   * @return {@code  levelsPointer().isCondensed()}
   */
  public final boolean isCondensed() {
    return
        levelsPointer().isCondensed();
  }


  /**
   * Determines whether all level row hashes are present.
   * If an in an instance is <em>not</em> condensed, then
   * all level row hashes are known by the instance.
   * 
   * @return {@code !isCondensed()}
   */
  public final boolean hasAllLevels() {
    return !isCondensed();
  }
  
  
//  /**
//   * Determines whether this row has the enough data to be at
//   * the head of a {@linkplain Path path}.
//   * <p>
//   * This is an implementation limitation that will be fixed in a future
//   * release. The issue is actually with the {@linkplain PathPack} class,
//   * and back-tracking from there, the issue boils down to <em>this class</em>
//   * not properly modeling the {@linkplain LevelsPointer#hash() levels
//   * pointer hash} using just a plain byte sequence. If it did, then
//   * <em>any</em> condensed row could be at the head of a path.
//   * </p>
//   * 
//   * @return    {@code levelsPointer().coversLevel(0)}
//   */
//  public final boolean isHeadable() {
//    return levelsPointer().coversLevel(0);
//  }



  /**
   * Tests whether the instance's hash is computed using
   * the minimum number of previous row hashes. Note, an instance
   * can both have {@linkplain #hasAllLevels() all levels} and
   * still be compressed.
   * 
   * @return {@code  levelsPointer().isCompressed()}
   */
  public final boolean isCompressed() {
    return levelsPointer().isCompressed();
  }
  
  
  /**
   * Returns the hash of the given row number.
   * 
   */
  public final ByteBuffer hash(long rowNumber) {
    return rowNumber == no() ? hash() : levelsPointer().rowHash(rowNumber);
  }
  
  

  // TODO:  API-wise need some such advertisement. Not used at this moment
  //        Same information can be inferred from introduced LevelsPointer,
  //        but commenting out, cuz it's 1 less thing to think about while
  //        refactoring

  // /**
  //  * Returns the set of row numbers covered by this row. This includes the row's
  //  * own row number, as well as those referenced thru its hash pointers. The hashes
  //  * of these referenced rows is available thru the {@linkplain #hash(long)} method.
  //  */
  // public final SortedSet<Long> coveredRowNumbers() {
  //   return SkipLedger.coverage(List.of(no()));
  // }
  

  /**
   * Returns the hash of the row referenced at the given level.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #prevLevels()}
   * 
   * @return non-null, 32-bytes wide
   */
  public final ByteBuffer prevHash(int level) {
    return levelsPointer().levelHash(level);
  }
  
  
}


