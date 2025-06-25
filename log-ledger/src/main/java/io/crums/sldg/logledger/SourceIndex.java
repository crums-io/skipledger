/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;

import io.crums.io.Opening;
import io.crums.io.sef.Alf;
import io.crums.sldg.src.SourceRow;
import io.crums.util.TaskStack;

/**
 * Random access (by row number) to {@linkplain SourceRow}s constructed from a log
 * file and an (offsets) index.
 * 
 * @see #rowCount()
 * @see #sourceRow(long)
 * @see #rowBytes(long)
 * @see #rowString(long)
 */
public class SourceIndex extends IndexedFile {
  

  /**
   * Creates and returns a new instance.
   * 
   * @param logFile             the log file
   * @param offsetsFile         the offsets index file
   */
  public static SourceIndex newInstance(
      File logFile, File offsetsFile, HashingRules rules)
          throws UncheckedIOException {
    
    try (var onFail = new TaskStack()) {
      
      var offCh = Files.openVersioned(offsetsFile, Opening.READ_ONLY);
      onFail.pushClose(offCh);
      
      Alf offsets = new Alf(offCh, Files.HEADER_LENGTH);
      
      var log = Opening.READ_ONLY.openChannel(logFile);
      onFail.pushClose(log);
      
      var indexedSource = new SourceIndex(log, offsets, rules);
      
      onFail.clear();
      return indexedSource;
      
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on loading log file %s ; offsets file %s : %s"
          .formatted(logFile, offsetsFile, iox.getMessage()),
          iox);
    }
  }
  
  
  
  private final SourceRowLogBuilder builder;
  private final boolean checkEol;

  /**
   * Package-private constructor: the {@linkplain Alf} argument
   * is not "exported" by module.
   * 
   * @param log         the log file
   * @param offsets     starting offsets of rows
   * @param rules       rules log file parsed and hashed with
   */
  SourceIndex(FileChannel log, Alf offsets, HashingRules rules) {
    super(log, offsets);
    this.builder = new SourceRowLogBuilder(rules);
    this.checkEol = rules.grammar().skipsLines();
  }
  
  
  /**
   * Returns the {@linkplain SourceRow} at the given no.
   * 
   * @param rowNo       &ge; 1 and &le; {@linkplain #rowCount()}
   * @return
   * @throws IndexOutOfBoundsException
   * @throws UncheckedIOException
   */
  public SourceRow sourceRow(long rowNo)
      throws IndexOutOfBoundsException, UncheckedIOException {
    
    return builder.buildRow(rowNo, rowString(rowNo));
    
  }
  
  
  
  /**
   * Returns the strings contents of the ledgered line.
   * This method is overridden so as to drop any "grammar-neutral"
   * content after the EOL char {@code '\n'}, if the grammar
   * skips lines.
   * 
   * @see Grammar#skipsLines()
   */
  @Override
  public final String rowString(long rowNo)
      throws IndexOutOfBoundsException, UncheckedIOException {
    String rowString = super.rowString(rowNo);
    assert !rowString.isEmpty();
    
    if (checkEol) {
      // drop any neutral content after EOL
      final int nl = rowString.indexOf('\n');

      //    if (nl == -1) {
      //      // excepting the last row every row must have a '\n'
      //      if (rowNo != rowCount())
      //        throw new IndexedSourceException(
      //            "row [%d] has no new-line char ('\\n') (quoted): '%s'"
      //            .formatted(rowNo, rowString));
      //    
      //    } else if (nl + 1 != rowString.length())
      //      rowString = rowString.substring(0, nl + 1);
      
      // implementation of commented out code above w/ fewer comps..
      if (nl + 1 != rowString.length()) {
        if (nl == -1) {
          // excepting the last row every row must have a '\n'
          if (rowNo != rowCount())
            throw new SourceIndexException(
                "row [%d] has no new-line char ('\\n') (quoted): '%s'"
                .formatted(rowNo, rowString));
        } else
          rowString = rowString.substring(0, nl + 1);
      }
    }
    return rowString;
  }
  
  

}



















