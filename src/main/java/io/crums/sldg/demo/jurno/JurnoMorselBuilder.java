/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.demo.jurno;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.MorselFile;
import io.crums.sldg.packs.LedgerMorselBuilder;
import io.crums.sldg.src.SourceRow;

/**
 * Builds a morsel from a {@linkplain ProtoJournal}.
 * 
 * <h3>Design Note</h3>
 * <p>
 * This builder uses a fixed {@linkplain LedgerMorselBuilder} internally. It deliberately
 * doesn't derive from it, so that there are fewer code paths to consider.
 * </p>
 */
public class JurnoMorselBuilder {
  
  private final ProtoJournal journal;
  private final LedgerMorselBuilder builder;

  /**
   * On construction the builder is initialized with the journal's {@linkplain Ledger#statePath()
   * state path}.
   * 
   * @param journal non-empty journal
   */
  public JurnoMorselBuilder(ProtoJournal journal) {
    this(journal, true);
  }
  
  
  
  public JurnoMorselBuilder(ProtoJournal journal, boolean checkState) {
    if (Objects.requireNonNull(journal, "null journal").getLedgeredLines() == 0)
      throw new IllegalArgumentException("empty journal: " + journal);
    
    this.journal = Objects.requireNonNull(journal, "null journal");
    if (checkState) {
      if (journal.getState() == null)
        journal.dryRun();
      if (journal.getState().needsMending())
        throw new IllegalStateException(journal.getTextFile() + " is out-of-sync with ledger");
    }
    this.builder = new LedgerMorselBuilder(journal.db(), journal.getTextFile().getName());
  }
  
  
  
  
  
  /**
   * Adds entries in canonical form for the given ledgered line numbers and returns the number of entries
   * actually added.
   * <p>
   * This method injects also injects any nearest beacon (row) or trailed row/crumtrail, if any. It
   * also injects ledger rows that together with existing rows in the morsel complete a shortest path
   * connectng all the above objects and the entry thru the highest row in the ledger. See also
   * {@linkplain LedgerMorselBuilder#addSourceRow(SourceRow)}.
   * </p>
   * 
   * @param lineNumbers (ledgerable) line numbers (already ledgered) in strictly ascending order
   * 
   * @return the number entries added (zero, if the entries are already added)
   */
  public int addSourcesByLineNumber(List<Integer> lineNumbers)
      throws IllegalArgumentException, HashConflictException {
    
    // (also validates the arguments)
    List<String> lines = journal.getLedgerableLines(lineNumbers);
    
    {
      int lastLine = lineNumbers.get(lineNumbers.size() - 1);
      if (lastLine > journal.db().size())
        throw new IllegalArgumentException("ledgerable line " + lastLine + " has not been ledgered");
    }
    
    int count = 0;
    
    for (int index = 0; index < lines.size(); ++index) {
      
      long rowNum = lineNumbers.get(index);
      String line = lines.get(index);
      line = journal.canonicalizeLine(line);
      
      if (builder.addSourceRow(new SourceRow(rowNum, line)))
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
