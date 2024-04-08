/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static io.crums.sldg.logs.text.LogledgeConstants.STATE_MRSL_EXT;
import static io.crums.sldg.logs.text.LogledgeConstants.STATE_MRSL_PREFIX;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.mrsl.MorselFile;

/**
 * 
 */
public class StateMorselFiler extends NumberFiler
    implements ContextedHasher.Context {

  /**
   * @param dir index directory (doesn't have to exist)
   * 
   * @see LogledgeConstants#STATE_MRSL_PREFIX
   */
  public StateMorselFiler(File dir) {
    super(dir, STATE_MRSL_PREFIX, STATE_MRSL_EXT);
  }
  
  
  /**
   * Returns the morsel with the highest row number, if any.
   */
  public Optional<MorselFile> getStateMorsel() {
    return lastFile().map(MorselFile::new);
  }
  
  
  public Optional<Row> getRow(long rn) {
    SkipLedger.checkRealRowNumber(rn);
    
    List<Long> hiRns = listFileRns(rn);
    
    if (hiRns.isEmpty() ||
        !SkipLedger.skipPathNumbers(1, hiRns.get(0)).contains(rn))
      return Optional.empty();
    
    var morsel = new MorselFile(rnFile(hiRns.get(0))).getMorselPack();
    return Optional.of( morsel.getRow(rn) );
  }
  
  
  
  public Optional<ByteBuffer> getRowHash(long rn) {
    SkipLedger.checkRealRowNumber(rn);
    
    List<Long> hiRns = listFileRns(rn);
    
    if (hiRns.isEmpty() ||
        !SkipLedger.skipPathCoverage(1, hiRns.get(0)).contains(rn))
      return Optional.empty();

    var morsel = new MorselFile(rnFile(hiRns.get(0))).getMorselPack();
    
    return Optional.of( morsel.rowHash(rn) );
  }
  
  
  
  // CONTEXT implementation checks existing state
  // Note, since row hashes are linked, we only check the hash
  // of the last row in any morsel file.

  
  private List<Long> knownRns = List.of();

  @Override
  public void init() {
    knownRns = listFileRns();
  }


  @Override
  public void observeLedgeredLine(Fro frontier, long offset)
      throws IOException, MorselConflictException {
    
    if (knownRns.isEmpty())
      return;
    
    final long rn = knownRns.get(0);
    
    if (rn > frontier.rowNumber())
      return;
    
    assert rn == frontier.rowNumber();
    
    var mFile = new MorselFile(rnFile(rn));
    
    if (! mFile.getMorselPack().rowHash(rn).equals(frontier.rowHash()))
      throw new MorselConflictException(rn, mFile.getFile());
    
    knownRns = knownRns.subList(1, knownRns.size());
  }


  @Override
  public void observeEndState(Fro fro) throws IOException {
    if (knownRns.isEmpty())
      return;
    
    assert fro.rowNumber() < knownRns.get(0);
    
    long mrn = knownRns.get(0);
    File file = rnFile(mrn);
    
    throw new MorselConflictException(
        mrn, file,
        "source is trimmed: row [%d] in morsel %s never reached"
        .formatted(mrn, file.toString()));
  }
  

}








