/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.morsel.tc.NotarizedRow;
import io.crums.sldg.src.Cell;
import io.crums.sldg.src.SaltScheme;
import io.crums.sldg.src.SourcePack;
import io.crums.sldg.src.SourceRow;
import io.crums.tc.BlockProof;
import io.crums.tc.ChainParams;
import io.crums.tc.Crumtrail;
import io.crums.util.Lists;

/**
 * Mutable morsel. Presently, one can only <em>add</em> information;
 * to remove or redact stuff you have to start from scratch.
 * 
 * @see #declareLedger(LedgerInfo, Path)
 * @see #declareLog(String, Path, URI, String)
 * @see #declareTimechain(String, BlockProof, URI, String)
 * @see #addPath(LedgerId, Path)
 * @see #initSourcePack(LedgerId, SaltScheme)
 * @see #addSourceRow(LedgerId, SourceRow)
 * @see #addReference(LedgerId, LedgerId, Reference)
 * @see #addNotarizedRow(LedgerId, LedgerId, NotarizedRow)
 */
public class MorselBuilder implements Morsel {

  private final TreeMap<Integer, NuggetBuilder> nuggets = new TreeMap<>();
  
  
  /**
   * Constructs an empty instance.
   * 
   * @see #isEmpty()
   */
  public MorselBuilder() {  }
  
  
  private NuggetBuilder nugget(LedgerId id) {
    var builder = nuggets.get(id.id());
    if (builder == null || !builder.id().equals(id))
      throw new IllegalArgumentException("unknown id " + id);
    return builder;
  }
  

  
  

  @Override
  public List<LedgerId> ids() {
    return
        nuggets.values().stream().map(NuggetBuilder::id).toList();
  }
  

  /**
   * @see #objectNug(LedgerId)
   */
  @Override
  public Nugget getNugget(LedgerId id) throws IllegalArgumentException {
    return Nugget.wrap(nugget(id));
  }
  
  
  /**
   * Returns the nugget with the given {@code id} as an {@linkplain ObjectNug}
   * (a validated {@code Nugget} instance).
   * 
   * @throws IllegalArgumentException
   *         if {@code id} is not equal to one {@linkplain #ids()}
   */
  public ObjectNug objectNug(LedgerId id) throws IllegalArgumentException {
    return nugget(id).build();
  }
  
  
  
  /**
   * Creates and returns a new {@linkplain LedgerType#TIMECHAIN TIMECHAIN}
   * ledger ID.
   * 
   * @param alias       unique across the nuggets in the morsel
   * @param blockProof  contains both the {@linkplain Path} and
   *                    {@linkplain ChainParams}
   * @param uri         optional. If not {@code null},
   *                    unique across all nuggets in the morsel
   * @param desc        optional description (may be {@code}
   */
  public LedgerId declareTimechain(
      String alias, BlockProof blockProof, URI uri, String desc) {
    
    return
        declareLedger(
            new TimechainInfo(blockProof.chainParams(), alias, uri, desc),
            blockProof.chainState());
  }
  
  
  
  /**
   * Creates and returns a new {@linkplain LedgerType#LOG LOG} type
   * ledger ID.
   * 
   * @param alias unique name across <em>this</em> morsel
   * @param path  skipledger (commitment) path
   * @param uri   optional. If not {@code null}, then unique across morsel
   * @param desc  optional. ({@code null} or blank means none)
   */
  public LedgerId declareLog(String alias, Path path, URI uri, String desc) {
    return declareLedger(new LogInfo(alias, uri, desc), path);
  }
  
  
  
  
  /**
   * Main ledger nugget declaration method. The other declaration methods
   * are convenience wrappers on this method.
   * 
   * @param ledger      non-integral part of a {@linkplain LedgerId}
   * @param path        skipledger (commitment) path
   * @return    a new ID containing {@code ledger} for its
   *            {@linkplain LedgerId#info() info()}
   * 
   * @see #declareLog(String, Path, URI, String)
   * @see #declareTimechain(String, BlockProof, URI, String)
   */
  public LedgerId declareLedger(LedgerInfo ledger, Path path) {
    
    Objects.requireNonNull(path, "path");
    
    if (findIdByAlias(ledger.alias()).isPresent())
      throw new IllegalArgumentException(
          "ledger alias '%s' is already defined in this morsel"
          .formatted(ledger.alias()));
    
    if (ledger.uri().isPresent() && findIdByUri(ledger.uri().get()).isPresent())
      throw new IllegalArgumentException(
          "ledger URI '%s' is already defined in this morsel"
          .formatted(ledger.uri().get()));
    
    int lastId = nuggets.isEmpty() ? 0 : nuggets.lastKey();
    var id = new LedgerId(lastId + 1, ledger);
    var builder = new NuggetBuilder(id, path);
    
    var prev = nuggets.put(id.id(), builder);
    if (prev != null)
      throw new ConcurrentModificationException(
          "detected on declaring ledger " + ledger);
    
    return id;
  }
  
  
  /**
   * Adds the given <em>intersecting</em> path for the nugget
   * with the given ID.
   * 
   * @param id    equal to an one returned (declared) by this instance
   * @param path  the (skipledger commitment) path
   * 
   * @return the highest row no. in the existing nugget (before the
   *         argument is added) whose hash is known by {@code path} (&ge; 1)
   *         
   * @throws IllegalArgumentException
   *         if {@code path} does not intersect with any existing path
   *         in the identified nugget
   *         
   * @throws HashConflictException
   *         {@code path} belongs to another ledger
   */
  public long addPath(LedgerId id, Path path)
      throws IllegalArgumentException, HashConflictException {
    
    return nugget(id).addPath(path);
  }
  
  
  /**
   * Sets the {@linkplain SaltScheme salt scheme} to be used in the nugget's
   * {@linkplain SourcePack}. Invoke this method before adding source rows.
   * 
   * @param id          the nugget's ID
   */
  public void initSourcePack(LedgerId id, SaltScheme saltScheme) {
    nugget(id).setSaltScheme(saltScheme);
  }
  
  
  /**
   * Adds the given source row to the identfied nugget and returns the
   * result. See {@linkplain NuggetBuilder#addSourceRow(SourceRow)} for
   * possible exceptions that may be thrown.
   * 
   * @param id          the nugget's ID
   * @param srcRow      the source row
   * 
   * @return {@code true} if added; {@code false}, if a source row with
   *         the same no. is already added.
   */
  public boolean addSourceRow(LedgerId id, SourceRow srcRow) {
    return nugget(id).addSourceRow(srcRow);
  }
  
  
  
  
  /**
   * Records the given crumtrail notarizing the specified row-hash, iff
   * it adds information, and returns the result. Since every row-hash is
   * linked to the all predecessor (lower numbered) row-hashes in the ledger,
   * notarizing a row also amounts to notarizing every row before it.
   * 
   * <h4>Conditional Addition &amp; Side Effects</h4>
   * <p>
   * The crumtrail is added only if the specified row no. is not already
   * notarized by the given timechain, or if the row is already notarized
   * by the timechain, but at a <em>later</em> date.
   * </p><p>
   * Note, on success (return value {@code true}), any "stale" (older) notarized
   * rows (at the given row no. or before) are dropped. This is maintained and
   * enforced by the invariant that (per timechain) the list of notarized rows
   * is ordered by both ascending row-no. and ascending witness UTC.
   * </p>
   * 
   * <h5>Timechain Blocks Added</h5>
   * <p>
   * If the above conditions are met for recording the notarized row, in the
   * specified ledger nugget, this method also conditionally adds to the
   * <em>timechain</em> nugget any necessary <em>timechain block proof</em>
   * (a dressed up skipledger {@code Path}) from the given {@linkplain Crumtrail}.
   * </p>
   * 
   * @param id          the ID of the ledger being notarized
   * @param rowNo       the row no. of the row whose hash is witnessed
   * @param timechain   the timechain ID
   * @param trail       the crumtrail witnessing the row-hash
   * 
   * @return {@code true} iff any information was added
   * 
   * @throws IllegalArgumentException
   *         if the morsel does not have sufficient information to validate
   *         the {@code trail}, e.g. if the trail's blockproof does not
   *         intersect with any of the timechain nugget's paths
   *         
   * @throws HashConflictException
   *         if the given {@code trail} conflicts with that
   *         recorded in the chain
   */
  public boolean addNotarizedRow(
      LedgerId id, long rowNo, LedgerId timechain, Crumtrail trail)
          throws IllegalArgumentException, HashConflictException {
    
    var nugget = nugget(id);
    
    assertTimechain(timechain);
    var timechainNugget = nugget(timechain);
    
    final long blockNo = blockNoForUtc(timechainNugget.id(), trail.crum().utc());
    assert trail.blockProof().chainState().hasRow(blockNo);
    
    Path tcPathToAdd;
    
    var multipath = timechainNugget.paths();
    if (multipath.hasRow(blockNo))
      tcPathToAdd = null;
    else {
      Path path = trail.blockProof().chainState();
      final long hcn = multipath.highestCommonNo(path);
      if (hcn == 0L)
        throw new IllegalArgumentException(
          "trail blockproof %s does not intersect with any existing path %s in %s"
          .formatted(path, multipath.paths(), timechain));
      if (path.isCondensed()) {
        // TODO: do better
        tcPathToAdd = path;
      } else if (hcn <= blockNo) {
        int hcnIndex = Collections.binarySearch(path.nos(), hcn);
        if (hcnIndex < 0)
          hcnIndex = -1 - hcnIndex;
        tcPathToAdd = path.subPath(hcnIndex);
      } else {
        int blockNoIndex = Collections.binarySearch(path.nos(), blockNo);
        assert blockNoIndex >= 0;       // redundant
        tcPathToAdd = path.subPath(blockNoIndex);
      }
    }
    
    boolean added =
        nugget.addNotarizedRow(
            timechain,
            NotarizedRow.forWitnessedRow(rowNo, trail));
    
    if (added && tcPathToAdd != null)
      timechainNugget.addPath(tcPathToAdd);     // won't fail: already checked
    
    return added;
  }
  
  
  private void assertTimechain(LedgerId timechain) {
    if (!timechain.type().isTimechain())
      throw new IllegalArgumentException(
          timechain + " is not a timechain; " + timechain.type());
  }
  
  
  
  private long blockNoForUtc(LedgerId timechain, long utc) {
    return
        ((TimechainInfo) timechain.info()).params()
        .blockNoForUtc(utc);
  }
  
  
  /**
   * Adds the given notarized row.
   * 
   * @param id          ledger nugget identifier
   * @param timechain   timechain ID
   * @param nr          the notarized row
   * @return            {@code true} iff any information was added
   * 
   * @throws IllegalArgumentException
   *         if the morsel does not have sufficient information to validate
   *         {@code nr}
   * @throws HashConflictException
   *         if {@code nr.}{@linkplain NotarizedRow#rowHash() rowHash()}
   *         is not equal to the row-hash recorded in the nugget, or if
   *         {@code nr.}{@linkplain NotarizedRow#cargoHash() cargoHash()}
   *         is not equal to that recorded in the timechain block
   */
  public boolean addNotarizedRow(
      LedgerId id, LedgerId timechain, NotarizedRow nr)
          throws IllegalArgumentException, HashConflictException {
    
    assertTimechain(timechain);
    
    var timechainNugget = nugget(timechain);
    
    long blockNo = blockNoForUtc(timechainNugget.id(), nr.utc());
    
    var block = timechainNugget.paths().findRow(blockNo).orElseThrow(
        () -> new IllegalArgumentException(
            "timechain block [%d] (for UTC %d) not found"
            .formatted(blockNo, nr.utc())));
    
    if (!block.inputHash().equals(nr.cargoHash()))
      throw new HashConflictException(
          "cargo hash for notarized row [%d] (UTC %d) conflicts with chain block [%d] input hash"
          .formatted(nr.rowNo(), nr.utc(), blockNo));
    
    return nugget(id).addNotarizedRow(timechain, nr);
  }
  
  
  
  /**
   * Adds the given {@linkplain Reference} <em>from</em> an entry (row or cell)
   * in one ledger nugget, <em>to</em> an entry in another nugget.
   * 
   * @param fromId      <em>from</em>-nugget identifier
   * @param toId        <em>to</em>-nugget identifier
   * @param ref         cross-ledger reference coordinates
   * 
   * @return            {@code false}, if the given reference is already 
   *                    recorded in this morsel
   *                    
   * @throws IllegalArgumentException
   *         if malformed, or if there is insufficient information in the
   *         morsel to validate
   */
  public boolean addReference(LedgerId fromId, LedgerId toId, Reference ref)
      throws IllegalArgumentException {
    
    var toNug = nugget(toId);
    
    Object expectedValue;
    
    if (ref.toCommit()) {
      
      // below would make for a nice error message
      // .. skipping for now, since rowHash(..) already fails if not covered
      
//      if (!toNug.paths().coversRow(ref.toRn()))
//        throw new IllegalArgumentException(
//            "hash of row[%d] in %s not known; referenced from row[%d:%d] in %s"
//            .formatted(
//                ref.toRn(), toId, ref.fromRn(), ref.fromColNo(), fromId));
      
      expectedValue = toNug.paths().rowHash(ref.toRow());
    
    } else {
      
      SourceRow toRow =
          toNug.sourcePack().flatMap(s -> s.findSourceByNo(ref.toRow()))
          .orElseThrow( () ->  new IllegalArgumentException(
              "referenced row [%d] in %s not known; referenced from [%d:%d] in %s"
              .formatted(
                  ref.toRow(), toId, ref.fromRow(), ref.fromCol(), fromId)));
      
      if (ref.sameContent()) {
        if (toRow.hasRedactions()) {
          throw new IllegalArgumentException(
            "same-content referenced source row [%d] in %s has redactions; from [%d] in %s"
            .formatted(ref.toRow(), toId, ref.fromRow(), fromId));
        }
        expectedValue = Lists.map(toRow.cells(), Cell::data);
        
      } else {
        
        assert ref.toSingleCell();

        if (ref.toCol() >= toRow.cells().size())
          throw new IndexOutOfBoundsException(
              "referenced cell [%d:%d] in %s (cell count %d): %s"
              .formatted(
                  ref.toRow(), ref.toCol(), toId, toRow.cells().size(), ref));
        
        Cell toCell = toRow.cells().get(ref.toCol());
        if (toCell.isRedacted())
          throw new IllegalArgumentException(
              "referenced cell [%d:%d] in %s is redacted: referenced from [%d:%d] in %s"
              .formatted(
                  ref.toRow(), ref.toCol(), toId, ref.fromRow(), ref.fromCol(), fromId));
        
        expectedValue = toCell.data();
      }
    }
    
    return nugget(fromId).addForeignRef(toId, ref, expectedValue);
  }





  /**
   * Returns {@code true} iff no information has been added.
   * 
   * @return {@code ids().isEmpty()}
   */
  public boolean isEmpty() {
    return ids().isEmpty();
  }
  
  
  
  
  public ObjectMug build() {
    return new ObjectMug(this);
  }
  
  
  
  
  
  
  
  
  
}


























