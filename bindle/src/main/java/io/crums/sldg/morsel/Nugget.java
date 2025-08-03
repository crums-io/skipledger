/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.morsel.tc.NotarizedRow;
import io.crums.sldg.morsel.tc.NotaryPack;
import io.crums.sldg.src.SourcePack;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * A nugget of proofs about the contents of a ledger.
 */
public interface Nugget {
  
  /**
   * Returns a wrapped view of the given implementation. This is useful
   * when the implementation provides other methods (such as mutators)
   * that ought not be exposed.
   * 
   * @param nugget      the implementation class
   * 
   * @return a view of the {@code nugget} argument
   */
  public static Nugget wrap(Nugget nugget) {
    Objects.requireNonNull(nugget);
    return new Nugget() {
      @Override
      public Optional<SourcePack> sourcePack() {
        return nugget.sourcePack();
      }
      @Override
      public List<ForeignRefs> refPacks() {
        return nugget.refPacks();
      }
      @Override
      public MultiPath paths() {
        return nugget.paths();
      }
      @Override
      public List<NotaryPack> notaryPacks() {
        return nugget.notaryPacks();
      }
      @Override
      public LedgerId id() {
        return nugget.id();
      }
    };
  }
  
  
  /**
   * Returns the ledger ID.
   */
  LedgerId id();
  
  
  /**
   * Returns the ledger type. Convenience method.
   * 
   * @return {@code id().info().type()}
   */
  default LedgerType type() {
    return id().info().type();
  }
  
  
  /**
   * Returns the set of intersecting (commitment) paths for the ledger.
   */
  MultiPath paths();
  
  
  /**
   * Returns the optional source-pack. Not all ledgers can have source packs.
   * For example, {@linkplain LedgerType#TIMECHAIN}s don't. Any source row in the
   * the returned object is backed by a (commitment) {@linkplain Path} containing
   * that row no. with <em>input-hash</em> matching the hash of the source row.
   * 
   * @see LedgerType#commitsOnly()
   * @see SourceRow#hash()
   * @see Row#inputHash()
   */
  Optional<SourcePack> sourcePack();
  
  
  /**
   * Returns the notary packs. Every {@linkplain NotarizedRow} is backed by
   * {@linkplain Path} data, asserting the hash witnessed is a ledger commitment hash
   * (at some row no.).
   */
  List<NotaryPack> notaryPacks();
  
  
  /** Returns the list of timechain IDs in {@linkplain #notaryPacks()}. */
  default List<LedgerId> notaryIds() {
    return Lists.map(notaryPacks(), NotaryPack::chainId);
  }
  
  
  /**
   * Returns the ledger's foreign references, if any.
   * 
   * @see ForeignRefs
   */
  List<ForeignRefs> refPacks();
  

  /** Returns the list of ledger IDs in {@linkplain #refPacks()}. */
  default List<LedgerId> refIds() {
    return Lists.map(refPacks(), ForeignRefs::foreignId);
  }

}
