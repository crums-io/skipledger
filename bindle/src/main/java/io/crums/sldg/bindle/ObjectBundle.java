/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import java.nio.ByteBuffer;

import io.crums.io.SerialFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Row;
import io.crums.sldg.src.Cell;
import io.crums.sldg.src.SourcePack;
import io.crums.sldg.src.SourceRow;
import io.crums.tc.ChainParams;
import io.crums.util.Lists;

/**
 * A well-formed, validated {@linkplain Bindle}.
 */
public class ObjectBundle extends BundleBase implements Bundle {
  
  private final ObjectNug[] nugs;

  /**
   * Constructs an instance using a {@linkplain BindleBuilder}.
   * Since the argument type is guaranteed to be well-formed,
   * 
   * @param builder not empty
   */
  public ObjectBundle(BindleBuilder builder) {
    super(builder);
    
    var ids = ids();
    this.nugs = new ObjectNug[ids.size()];
    for (int index = 0; index < nugs.length; ++index)
      this.nugs[index] = builder.objectNug(ids.get(index));
  }
  
  
  /**
   * Constructs an instance from the given {@linkplain BundleBase}
   * implementation instance.
   * 
   * 
   * @param bindle      a purported bindle
   * 
   * @throws MalformedBindleException
   *         if the argument is malformed
   * @throws HashConflictException
   *         if a hash-proof in the argument fails
   */
  public ObjectBundle(BundleBase bindle)
      throws MalformedBindleException, HashConflictException {
    super(bindle);
    this.nugs = new ObjectNug[ids().size()];
    init(bindle);
  }
  
  
  
  
  
  /**
   * Constructs an instance from the given {@linkplain Bindle}.
   * 
   * @param bindle      a purported bindle
   * 
   * @throws MalformedBindleException
   *         if the argument is malformed
   * @throws HashConflictException
   *         if a hash-proof in the argument fails
   */
  public ObjectBundle(Bindle bindle)
      throws MalformedBindleException, HashConflictException {
    
    super(bindle.ids());
    
    this.nugs = new ObjectNug[ids().size()];
    init(bindle);
  }
  

  private void init(Bindle bindle) {
    var ids = ids();
    for (int index = 0; index < ids.size(); ++index) {
      Nugget nugget = bindle.getNugget(ids.get(index));
      // note: below, not checking for NuggetBuilder instance since
      //       the other constructor should take care of such a code path  
      this.nugs[index] =
          nugget instanceof ObjectNug nug ?
              nug :
                new ObjectNug(nugget);
    }
    
    verify();
  }
  
  
  private void verify()
      throws MalformedBindleException, HashConflictException {
    
    for (var nug : nugs) {
      verifyReferences(nug);
      verifyNotaries(nug);
    }
  }


  private void verifyReferences(ObjectNug nug)
      throws MalformedReferenceException, HashConflictException {
    
    if (nug.sourcePack().isEmpty())
      return;   // implementation guarantees nug.refPacks() is empty

    SourcePack nugSource = nug.sourcePack().get();
    
    for (var foreignRefs : nug.refPacks()) {
      
      final LedgerId foreignId = foreignRefs.foreignId();
      assert !foreignId.equals(nug.id());       // (implementation guarantees)
      
      final Nugget foreignNug;
      try {
        foreignNug = getNugget(foreignId);
        
      } catch (Exception x) {
        throw new MalformedReferenceException(
            "%s: referenced foreign nugget %s not found; %s"
            .formatted(nug.id(), foreignId, x), x);
      }
      
      SourceRow currentRow = nugSource.sources().getFirst();
      
      for (var ref : foreignRefs.refs()) {
        
        if (currentRow.no() != ref.fromRow())
          // (ObjectNug guarantees source row exists)
          currentRow = nugSource.sourceByNo(ref.fromRow());
        
        if (ref.toCommit()) {
          
          if (!foreignNug.paths().coversRow(ref.toRow()))
            throw new MalformedReferenceException(
                "%s: referenced row-hash [%d] in %s is not known: %s"
                .formatted(nug.id(), ref.toRow(), foreignId, ref));
          var colData = currentRow.cells().get(ref.fromCol()).data();
          var rowHash = foreignNug.paths().rowHash(ref.toRow());
          if (!colData.equals(rowHash))
            throw new HashConflictException(
                "%s: row-hash [%d] in %s conflicts with that indicated in %s"
                .formatted(nug.id(), ref.toRow(), foreignId, ref));
          continue;
        }
          
        SourceRow foreignRow =
            foreignNug.sourcePack().flatMap(s -> s.findSourceByNo(ref.toRow()))
            .orElseThrow(
                () -> new MalformedReferenceException(
                    "%s: source-row [%d] in %s not found: %s"
                    .formatted(nug.id(), ref.toRow(), foreignId, ref)));
        
        if (ref.sameContent()) {
          if (foreignRow.hasRedactions())
            throw new MalformedReferenceException(
                "%s: source-row [%d] in %s has redactions: %s"
                .formatted(nug.id(), ref.toRow(), foreignId, ref));
          var expected = Lists.map(foreignRow.cells(), Cell::data);
          var actual = Lists.map(currentRow.cells(), Cell::data);
          if (!expected.equals(actual))
            throw new MalformedReferenceException(
              "%s: source-rows differ for same-content reference to %s: from %s to %s"
              .formatted(nug.id(), foreignId, currentRow, foreignRow));
          continue;
        }
          
        assert ref.toSingleCell();
        
        if (ref.toCol() >= foreignRow.cells().size())
          throw new MalformedReferenceException(
            "%s: out-of-bounds foreign cell index [%d:%d] in %s: foreign row %s"
            .formatted(
                nug.id(), ref.toRow(), ref.toCol(), foreignId, foreignRow));
        
        Cell foreignCell = foreignRow.cells().get(ref.toCol());
        if (foreignCell.isRedacted())
          throw new MalformedReferenceException(
            "%s: foreign cell [%d:%d] in %s is redacted: %s; foreign row %s"
            .formatted(
                nug.id(), ref.toRow(), ref.toCol(), foreignId, ref, foreignRow));
        
        Cell fromCell = currentRow.cells().get(ref.fromCol());
        
        if (!fromCell.data().equals(foreignCell.data()))
          throw new MalformedReferenceException(
              "%s: cell values for %s to %s differ; from %s to %s"
              .formatted(nug.id(), ref, foreignId, currentRow, foreignRow));
        
        
      } // for (var ref : foreignRefs.refs())
    } // for (var foreignRefs : nug.refPacks())
  }


  private void verifyNotaries(ObjectNug nug)
      throws MalformedNotarizedRowException, HashConflictException {
    
    for (var notaryPack : nug.notaryPacks()) {
      
      MultiPath chainBlocks = getNugget(notaryPack.chainId()).paths();
      ChainParams chainParams = notaryPack.chainParams();
      
      for (var notarizedRow : notaryPack.notarizedRows()) {
        
        long blockNo = chainParams.blockNoForUtc(notarizedRow.utc());
        Row block = chainBlocks.findRow(blockNo).orElseThrow(
            () -> new MalformedNotarizedRowException(
              "%s: block [%d] in timechain %s for notarized row [%d] not found"
              .formatted(nug.id(), blockNo, notaryPack.chainId())));
        if (!notarizedRow.cargoHash().equals(block.inputHash()))
          throw new HashConflictException(
            "%s: cargo hash for notarized row [%d] must equal input-hash in block [%d] of %s"
            .formatted(
                nug.id(), notarizedRow.rowNo(), blockNo, notaryPack.chainId()));
        
      }
    }
  }


  

  /**
   * @return a validated {@linkplain Nugget}
   */
  @Override
  public final ObjectNug getNugget(LedgerId id) throws IllegalArgumentException {
    return nugs[indexOf(id)];
  }
  
  
  
  /**
   * Loads and returns an instance from serial form.
   * 
   * @return {@code new ObjectBundle( LazyBun.load(in) )}
   */
  public static ObjectBundle load(ByteBuffer in)
      throws
      SerialFormatException,
      MalformedBindleException, HashConflictException {
    
    var bindle = LazyBun.load(in);
    return new ObjectBundle(bindle);
  }

}








