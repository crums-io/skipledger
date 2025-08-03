/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.morsel.tc.NotarizedRow;
import io.crums.sldg.morsel.tc.NotaryPack;
import io.crums.sldg.src.Cell;
import io.crums.sldg.src.SaltScheme;
import io.crums.sldg.src.SourcePack;
import io.crums.sldg.src.SourcePackBuilder;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * Nugget builder.
 * 
 * 
 */
public final class NuggetBuilder implements Nugget {

  private final List<NotaryPack.Builder> notaries = new ArrayList<>();
  private final List<ForeignRefs.Builder> refs = new ArrayList<>();

  private final LedgerId id;
  private final MultiPathBuilder paths;
  
  private SourcePackBuilder sources;
  
  
  /**
   * Constructs an instance with the given starting (commitment) path.
   */
  public NuggetBuilder(LedgerId id, Path path) {
    this.paths = new MultiPathBuilder(path);
    this.id = id;
  }
  
  
  
  

  public ObjectNug build() {
    return new ObjectNug(this);
  }
  
  @Override
  public LedgerId id() {
    return id;
  }


  @Override
  public MultiPath paths() {
    return paths.toMultiPath();
  }


  @Override
  public Optional<SourcePack> sourcePack() {
    return sources == null || sources.isEmpty() ?
        Optional.empty() :
          Optional.of(sources.build());
  }


  @Override
  public List<NotaryPack> notaryPacks() {
    return notaries.stream().map(NotaryPack.Builder::build).toList();
  }


  @Override
  public List<ForeignRefs> refPacks() {
    return refs.stream().map(ForeignRefs.Builder::build).toList();
  }
  
  /**
   * Adds the given <em>intersecting</em> path and returns the highest row number
   * they intersect at.
   * 
   * @param path intersects at least one existing path in the collection
   * 
   * @return the highest row no. in the existing collection (before the
   *         argument is added) whose hash is known by {@code path} (&ge; 1)
   * @throws IllegalArgumentException
   *         if {@code path} does not intersect one of the paths in the
   *         paths in the instance
   * @throws HashConflictException
   *         if the hash at any row no in the given {@code path} conflicts
   *         with what's recorded in this collection
   * 
   * @see MultiPathBuilder#addPath(Path)
   * @see #getPaths()
   */
  public long addPath(Path path)
      throws IllegalArgumentException, HashConflictException {
    return paths.addPath(path);
  }
  
  
  /**
   * Returns a read-only snapshot of the (commitment) paths thus far
   * added.
   * 
   * @see #addPath(Path)
   */
  public List<Path> getPaths() {
    return Lists.asReadOnlyList(paths.paths());
  }
  
  
  
  /**
   * Sets the {@linkplain SaltScheme salt scheme} to be used in the nugget's
   * {@linkplain SourcePack}. Invoke this method before adding source rows.
   * 
   * @throws UnsupportedOperationException
   *         if the ledger is a {@linkplain LedgerType#TIMECHAIN} or a
   *         {@linkplain LedgerType#BSTREAM}
   *         
   * @see #addSourceRow(SourceRow)
   */
  public void setSaltScheme(SaltScheme saltScheme) throws UnsupportedOperationException {
    
    if (sources == null) {
      checkSourceAllowed();
      
      if (id.type().isLog() && saltScheme.isMixed()) {
        throw new IllegalArgumentException(
            "mixed salt scheme for LOG id %s not supported"
            .formatted(id));
      }
      this.sources = new SourcePackBuilder(saltScheme);
    
    } else if (!sources.saltScheme().equals(saltScheme))
      throw new IllegalStateException(
          "instance already initialized with other salt scheme");
    // else, it's a no-op
  }
  
  
  public Optional<SaltScheme> saltScheme() {
    return sources == null ? Optional.empty() : Optional.of(sources.saltScheme());
  }
  
  
  
  /**
   * Adds the given source row if not already added and returns the result.
   * The input-hash for the given row no. must already be recorded in one or
   * more of the paths {@linkplain #addPath(Path) added}. The salt scheme
   * must be set before this method may be invoked.
   * 
   * @param srcRow the source row to be added, consistent with the set
   *               {@linkplain #saltScheme()}
   * 
   * @return {@code true} iff {@code srcRow} was added
   * @throws IllegalArgumentException
   *         if the input-hash at the given row no. is not known
   * @throws IllegalStateException
   *         if {@linkplain #setSaltScheme(SaltScheme)} has not been invoked
   * @throws HashConflictException
   *         if {@code srcRow.hash()} does not match the recorded input-hash in
   *         the (commitment) path for that row no.
   * @throws UnsupportedOperationException
   *         if {@code id().info().type().commitsOnly()} is {@code true}. E.g.
   *         if the ledger is in-fact a timechain.
   *         
   * @see #setSaltScheme(SaltScheme)
   * @see SourcePackBuilder#add(SourceRow)
   */
  public boolean addSourceRow(SourceRow srcRow) throws HashConflictException {
    
    if (sources == null) {
      checkSourceAllowed();
      throw new IllegalStateException("salt scheme not set");
    }
    
    Row row = paths.findRow(srcRow.no()).orElseThrow(
        () -> new IllegalArgumentException(
            "input (source row) hash for row [%d] not known for ledger %s"
            .formatted(srcRow.no(), id)));
    
    if (!row.inputHash().equals(srcRow.hash()))
      throw new HashConflictException(
          "srcRow hash conflicts with input hash committed in row [%d]"
          .formatted(srcRow.no()));
    
    return sources.add(srcRow);
  }
  
  
  
  private void checkSourceAllowed() {
    if (id.info().type().commitsOnly())
      throw new UnsupportedOperationException(
          "ledger type " + id.info().type() +" does not have source rows");
  }
  
  
  
  /**
   * Adds the given notarized row from the timechain with the given id
   * establishing the minimum age of the row. The hash of the given
   * notarized row must already be known (via
   * {@linkplain #paths()}.{@linkplain MultiPath#rowHash(long) rowHash(nr.rowNo())}.
   * 
   * @param chainId timechain ID
   * @param nr the notarized row
   * @return {@code true} iff any information was added
   * 
   * @throws IllegalArgumentException
   *         if a timechain is notarizing itself (tho a timechain's blocks can be
   *         notarized by other timechains);
   *         if the row-hash for {@code nr.rowNo()} is not known
   * @throws HashConflictException
   *         if {@code nr.rowHash()} conflicts with that recorded in a
   *         (commitment) path
   *         
   * @see NotaryPack.Builder#addNotarizedRow(NotarizedRow)
   */
  public boolean addNotarizedRow(LedgerId chainId, NotarizedRow nr)
  throws IllegalArgumentException, HashConflictException {
    
    Objects.requireNonNull(nr, "nr");
    if (chainId.id() == id.id())
      throw new IllegalArgumentException(
          "chainId %d same as instance id %d"
          .formatted(chainId.id(), id.id()));
    
    
    if (!nr.rowHash().equals(paths.rowHash(nr.rowNo())))
      throw new HashConflictException(
          "notarized row hash conflicts with existing hash for row [%d]"
          .formatted(nr.rowNo()));
    
    NotaryPack.Builder builder;
    {
      var existing =
          notaries.stream()
          .filter(b -> b.chainId().id() == chainId.id()).findAny();
      
      if (existing.isPresent())
        builder = existing.get();
      else {
        builder = new NotaryPack.Builder(chainId);
        notaries.add(builder);
      }
    }
    
    return builder.addNotarizedRow(nr);
  }
  
  
  
  
  
  /**
   * Adds the given foreign reference from an already added source row to
   * another row in a foreign ledger and returns the result.
   * 
   * @param foreignId     id for foreign ledger
   * @param ref           the source row for the given reference must already
   *                      be added
   * 
   * @return {@code true} iff {@code ref} was not already in the collection.
   * 
   * @throws IllegalArgumentException
   *         if the referenced source row is not in the collection
   * @throws IndexOutOfBoundsException
   *         if the referenced source row does not contain the referenced cell
   * @throws UnsupportedOperationException
   *         if {@code id().info().type().commitsOnly()} is {@code true}. E.g.
   *         if the ledger is in-fact a timechain.
   * 
   * @see #addSourceRow(SourceRow)
   * @see Reference
   * @see #addForeignRef(LedgerId, Reference, ByteBuffer)
   */
  public boolean addForeignRef(LedgerId foreignId, Reference ref)
      throws IllegalArgumentException {
    return addForeignRef(foreignId, ref, null);
  }
  

  /**
   * Adds the given foreign reference from an already added source row to
   * another row in a foreign ledger and returns the result.
   * 
   * @param foreignId     id for foreign ledger
   * @param ref           the source row for the given reference must already
   *                      be added
   * @param expectedValue if provided (not {@code null}), then the source
   *                      cell value is checked against
   * 
   * @return {@code true} iff {@code ref} was not already in the collection.
   * 
   * @throws IllegalArgumentException
   *         if the referenced source row is not in the collection
   * @throws IndexOutOfBoundsException
   *         if the referenced source row does not contain the referenced cell
   * @throws UnsupportedOperationException
   *         if {@code id().info().type().commitsOnly()} is {@code true}. E.g.
   *         if the ledger is in-fact a timechain.
   * 
   * @see #addSourceRow(SourceRow)
   * @see Reference
   */
  public boolean addForeignRef(
      LedgerId foreignId, Reference ref, Object expectedValue)
          throws IllegalArgumentException {

    checkSourceAllowed();
    if (foreignId.id() == id.id())
      throw new IllegalArgumentException(
          "foreignId %d same as instance id %d"
          .formatted(foreignId.id(), id.id()));
    
    if (sources == null)
      throw new IllegalArgumentException(
          "source row [%d] not found (no source rows added): %s"
          .formatted(ref.fromRow(), ref));
    
    SourceRow srcRow = sources.findSourceByNo(ref.fromRow()).orElseThrow(
        () -> new IllegalArgumentException(
            "source row [%d] for foreign ref %s not found"
            .formatted(ref.fromRow(), ref)) );
    
    if (ref.sameContent()) {
      
      if (srcRow.hasRedactions())
        throw new IllegalArgumentException(
            "source row [%d] for same-content foreign ref %s has redacted cells"
            .formatted(ref.fromRow(), ref));
      if (expectedValue != null &&
          !Lists.map(srcRow.cells(), Cell::data).equals(expectedValue)) {
        throw new IllegalArgumentException(
            "source row contents conflict with same-content foreign ref %s: %s"
            .formatted(ref, srcRow));
      }
    } else {

      var cells = srcRow.cells();
      if (ref.fromCol() >= cells.size())
        throw new IndexOutOfBoundsException(
            "ref.fromCol() %d; source cell-count is %d"
            .formatted(ref.fromCol(), cells.size()));
      
      Cell fromCell = cells.get(ref.fromCol());
      if (fromCell.isRedacted())
        throw new IllegalArgumentException(
            "source row-cell [%d:%d] for foreign ref %s is redacted"
            .formatted(ref.fromRow(), ref.fromCol(), ref));
      
      if (expectedValue != null && !fromCell.data().equals(expectedValue))
        throw new HashConflictException(
            "source cell value conflicts with referenced value in %s: %s"
            .formatted(foreignId, ref));
    }
    
    
    ForeignRefs.Builder builder;
    {
      var existing =
          refs.stream()
          .filter(b -> b.foreignId().id() == foreignId.id()).findAny();
      
      if (existing.isPresent())
        builder = existing.get();
      else {
        builder = new ForeignRefs.Builder(foreignId);
        refs.add(builder);
      }
    }
    
    return builder.add(ref);
    
  }


  

}










