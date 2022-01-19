/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.demo.jurno;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;

import io.crums.sldg.SourceLedger;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * An append-only text file (the journal) as a source-ledger. Supports word-by-word
 * redactions in morsel output.
 * 
 * <h2>Parsing Rules</h2>
 * <p>
 * With the exception of the first line, which is treated as a neutral and doesn't count,
 * each non-blank line of text is represented as a row in the ledger. The column values in
 * each row, in turn, encode the sequence of whitespace-delimited tokens in that line. In
 * most cases, these tokens will be ordinary words.
 * </p><p>
 * The upshot of these rules are the following:
 * <ul>
 * <li>Empty or blank lines don't count (you can interleave new empty lines after-the-fact
 * and it won't break anything).</li>
 * <li>The number of spaces, tabs, etc. between tokens (words) doesn't matter. Indentation, for e.g.,
 * doesn't matter.</li>
 * <li>The first line doesn't count. You can use it as a comment line that can be edited.</li>
 * </ul>
 * </p>
 * <h2>Implementation</h2>
 * <p>
 * At load time (instantiation), the instance parses the file to create an index of ledgerable
 * lines and their offsets.
 * </p>
 */
public class TextFileSource implements SourceLedger {
  
  /**
   * Tracks the line number and offset. Its row number is
   * determined by its position in {@link TextFileSource#lineOffsets}.
   */
  private static class LineOffset {
    final long offset;
    final int lineNo;
    
    LineOffset(long offset, int lineNo) {
      this.offset = offset;
      this.lineNo = lineNo;
    }
  }
  
  
  private final ArrayList<LineOffset> lineOffsets = new ArrayList<>(128);
  
  private final File file;
  
  private final TableSalt shaker;
  
  private final RandomAccessFile raf;
  
  private int linesInFile;

  /**
   * 
   */
  public TextFileSource(File file, TableSalt shaker) {
    this.file = Objects.requireNonNull(file, "null file");
    this.shaker = Objects.requireNonNull(shaker, "null shaker");
    try {
      this.raf = new RandomAccessFile(file, "r");
    } catch (IOException iox) {
      throw new UncheckedIOException("failed to open text file " + file, iox);
    }
    refresh();
  }
  
  
  
  public final File getFile() {
    return file;
  }
  
  
  public synchronized int getLinesInFile() {
    return linesInFile;
  }
  
  
  
  public synchronized int lineNumber(long rowNumber) {
    if (rowNumber > lineOffsets.size() || rowNumber < 1)
      throw new IllegalArgumentException("rowNumber out-of-bounds: " + rowNumber);
    return lineOffsets.get((int) rowNumber - 1).lineNo;
  }
  
  
  public synchronized int lineToRowNumber(int lineNumber) {
    List<Integer> ledgeredLines = Lists.map(lineOffsets, loff -> loff.lineNo);
    int si = Collections.binarySearch(ledgeredLines, lineNumber);
    return si < 0 ? 0 : si + 1;
  }
  
  
  /**
   * Refreshes the row-count, line numbers, and offsets.
   * 
   * @throws UncheckedIOException if an I/O error occurs, in which case the instance is automatically
   *          closed on throwing this exception
   */
  public synchronized void refresh() throws UncheckedIOException {
    try  {
      raf.seek(0);
      lineOffsets.clear();
      linesInFile = 0;
      
      // the first line is skipped: it can count as a neutral comment line
      //
      // (this is not really an intended feature: reason why I'm doing this
      // is I don't want to worry about funky file header bytes)
      while (true) {
        int b = raf.read();
        if (b == '\n')
          break;
        else if (b == -1)
          return;
      }
      
      long pos = raf.getFilePointer();
      long lineOffset = pos;
      int lineNo = 2;
      boolean nonspaceHit = false;
      
      
      
      while (true) {
        ++pos;
        
        switch (raf.read()) {
        case '\t':
        case '\f':
        case '\r':
        case ' ':
          break;
        case '\n':
          if (nonspaceHit)
            lineOffsets.add(new LineOffset(lineOffset, lineNo));
          
          lineOffset = pos;
          ++lineNo;
          assert lineNo > 1;  // 2GB (of lines) OVERFLOW check
          nonspaceHit = false;
          break;
        
        case -1:
          // EOF
          if (nonspaceHit)
            lineOffsets.add(new LineOffset(lineOffset, lineNo));
          
          linesInFile = lineNo;
          return;
        
        default:
          nonspaceHit = true;
        }
        
      } // while
      
    } catch (IOException iox) {
      close();
      throw new UncheckedIOException("on refresh()", iox);
    }
  }

  @Override
  public void close() {
    try (TaskStack closer = new TaskStack()) {
      closer.pushClose(raf).pushClose(shaker).close();
    }
  }

  @Override
  public synchronized long size() {
    return lineOffsets.size();
  }

  /**
   * {@inheritDoc}
   * 
   * <h3>Implementation Note</h3>
   * <p>
   * Each invocation causes a disk access. If the file has been modified since construction,
   * or the last {@linkplain #refresh()} (appends are OK), then results are undefined.
   * </p><p>
   * Unlike {@linkplain #refresh()}, I/O errors in this method <em>do not</em> automatically
   * {@linkplain #close() close} the instance. The rationale is that if the text file is
   * changed from an external process, and an I/O error occurs, then the instance may
   * recover with a refresh.
   * </p>
   */
  @Override
  public synchronized SourceRow getSourceRow(long rn) {
    
    String line = rawRowText(rn);
    ArrayList<ColumnValue> words = new ArrayList<>();
    int col = 0;
    for ( var tokenizer = new StringTokenizer(line);
          tokenizer.hasMoreTokens(); )
      words.add(new StringValue(tokenizer.nextToken(), shaker.salt(rn, ++col)));
    
    assert !words.isEmpty();
    
    return new SourceRow(rn, words);
  }
  
  
  
  /**
   * Returns the text on the line with the given row number.
   * 
   * @param rn row-number, not line-number.
   * 
   * @return non-null, non-empty line of text (as is, uncanonicalized)
   */
  public synchronized String rawRowText(long rn) {
    if (rn < 1 || rn > lineOffsets.size())
      throw new IllegalArgumentException("rn out-of-bounds: " + rn);
    
    
    byte[] lineBytes;
    try {

      final int rowNum = (int) rn;
      long offset = lineOffsets.get(rowNum - 1).offset;
      long endOff = rowNum == lineOffsets.size() ? raf.length() : lineOffsets.get(rowNum).offset;
      
      lineBytes = new byte[(int)(endOff - offset)];

      raf.seek(offset);
      raf.readFully(lineBytes);
      
    } catch (IOException iox) {
      throw new UncheckedIOException("on rawRowText(" + rn + ")", iox);
    }
    
    return new String(lineBytes, Strings.UTF_8);
  }

}






