/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.demo.jurno;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.Ledger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.db.Db;
import io.crums.sldg.db.DbMorselBuilder;
import io.crums.sldg.db.MorselFile;
import io.crums.util.Strings;

/**
 * Builds a morsel from a {@linkplain Journal}.
 * 
 * <h3>Design Note</h3>
 * <p>
 * This builder uses a fixed {@linkplain DbMorselBuilder} internally. It deliberately
 * doesn't derive from it, so that there are fewer code paths to consider.
 * </p>
 */
public class JurnoMorselBuilder {
  
  private final Journal journal;
  private final DbMorselBuilder builder;

  /**
   * On construction the builder is initialized with the journal's {@linkplain Ledger#statePath()
   * state path}.
   * 
   * @param journal non-empty journal
   */
  public JurnoMorselBuilder(Journal journal) {
    this(journal, true);
  }
  
  
  
  public JurnoMorselBuilder(Journal journal, boolean checkState) {
    if (Objects.requireNonNull(journal, "null journal").getLedgeredLines() == 0)
      throw new IllegalArgumentException("empty journal: " + journal);
    
    this.journal = Objects.requireNonNull(journal, "null journal");
    if (checkState) {
      if (journal.getState() == null)
        journal.dryRun();
      if (journal.getState().needsMending())
        throw new IllegalStateException(journal.getTextFile() + " is out-of-sync with ledger");
    }
    this.builder = new DbMorselBuilder(journal.db());
  }
  
  
  
  
  
  /**
   * Adds entries in canonical form for the given ledgered line numbers and returns the number of entries
   * actually added.
   * <p>
   * This method injects also injects any nearest beacon (row) or trailed row/crumtrail, if any. It
   * also injects ledger rows that together with existing rows in the morsel complete a shortest path
   * connectng all the above objects and the entry thru the highest row in the ledger. See also
   * {@linkplain DbMorselBuilder#addEntry(long, ByteBuffer, String)}.
   * </p>
   * 
   * @param lineNumbers (ledgerable) line numbers (already ledgered) in strictly ascending order
   * 
   * @return the number entries added (zero, if the entries are already added)
   */
  public int addEntriesByLineNumber(List<Integer> lineNumbers)
      throws IllegalArgumentException, HashConflictException {
    
    return addEntriesByLineNumber(lineNumbers, true);
  }
  
  /**
   * Adds entries for the given ledgered line numbers and returns the number of entries
   * actually added.
   * <p>
   * This method injects also injects any nearest beacon (row) or trailed row/crumtrail, if any. It
   * also injects ledger rows that together with existing rows in the morsel complete a shortest path
   * connectng all the above objects and the entry thru the highest row in the ledger. See also
   * {@linkplain DbMorselBuilder#addEntry(long, ByteBuffer, String)}.
   * </p>
   * 
   * @param lineNumbers (ledgerable) line numbers (already ledgered) in strictly ascending order
   * @param canonicalForm if {@code true} (the default) then each ledgerable line is first transformed
   *        into canonical form before being added to the morsel
   * 
   * @return the number entries added (zero, if the entries are already added)
   */
  public int addEntriesByLineNumber(List<Integer> lineNumbers, boolean canonicalForm)
      throws IllegalArgumentException, HashConflictException {
    
    // (also validates the arguments)
    List<String> lines = journal.getLedgerableLines(lineNumbers);
    
    {
      int lastLine = lineNumbers.get(lineNumbers.size() - 1);
      if (lastLine > journal.db().sizeSansBeacons())
        throw new IllegalArgumentException("ledgerable line " + lastLine + " has not been ledgered");
    }
    
    int count = 0;
    Db db = journal.db();
    MessageDigest digest = SldgConstants.DIGEST.newDigest();
    
    for (int index = 0; index < lines.size(); ++index) {
      
      long rowNum = db.rowNumWithBeacons(lineNumbers.get(index));
      String line = lines.get(index);
      
      // make sure the canonical form of the line hasn't changed;
      // and turn line into canonical form, if so flagged
      {
        String canonical = journal.canonicalizeLine(line);
        assert canonical != null; // else, it should have been filtered
        
        if (!journal.verifyLine(canonical, rowNum, digest))
          throw new HashConflictException(
              "line " + lineNumbers.get(index) + " does not match ledger hash at row " + rowNum);
        
        if (canonicalForm)
          line = canonical;
      }
      
      ByteBuffer bytes = ByteBuffer.wrap(line.getBytes(Strings.UTF_8));
      if (builder.addEntry(rowNum, bytes, null))
        ++count;
    }
    
    return count;
  }
  
  
  
  /**
   * Creates a new morsel file using the state of this instance.
   * 
   * <h2>Auto Filename Generation</h2>
   * 
   * <p>TODO: document if this becomes a thing.</p>
   * 
   * @param file path to non-existent file (suggest <em>.mrsl</em> extension) <em>or</em>
   *             path to an existing directory (in which case a filename is generated on
   *             the fly).
   *             
   * @return the morsel file
   */
  public File createMorselFile(File file) throws IOException {
    return MorselFile.createMorselFile(file, builder);
  }

}
