/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.logledger.LogParser.Listener;
import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.SaltScheme;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.SourceRowBuilder;

/**
 * A {@linkplain Listener LogParser.Listener} that builds and gathers ledgerable
 * lines in a log as {@linkplain SourceRow}s.
 */
public class SourceRowGatherer implements Listener {
  
  
  /**
   * Creates and returns a selector that matches the row no.s in the given
   * set. Arguments are not checked.
   * 
   * 
   * @param selected    not {@code null}
   * 
   * @return {@code selected::contains}
   */
  public static Predicate<Long> selectSet(Set<Long> selected) {
    return selected::contains;
  }
  
  
  
  /**
   * Creates and returns a selector that matches the elements of the given
   * collection. If the collection's size is 8 or greater, then
   * {@linkplain #selectSet(Set) selectSet}{@code (new HashSet<>(selected)}
   * is returned.
   */
  public static Predicate<Long> selectCollection(Collection<Long> selected) {
    if (selected.size() < 8)
      return selected::contains;
    
    Set<Long> set =
        selected instanceof Set<Long> s ? s :
          new HashSet<>(selected);
    
    return selectSet(set);
  }
  
  /**
   * Creates and returns a range selector. Arguments are not checked.
   * 
   * @param begin       beginning row no. (inclusive)
   * @param end         ending row no. (inclusive)
   * 
   */
  public static Predicate<Long> selectRange(long begin, long end) {
    return r -> r >= begin && r <= end;
  }
  
  
  /**
   * Creates and returns selector using the given row numbers.
   * Arguments are not checked.
   * 
   * @param rowNos 
   * @return
   */
  public static Predicate<Long> selectRows(long... rowNos) {
    if (rowNos.length < 8)
      return r -> contains(r, rowNos);
      
    Arrays.sort(rowNos);
    return r -> Arrays.binarySearch(rowNos, r) >= 0;
  }
  
  
  private static boolean contains(long r, long[] rowNos) {
    int index = rowNos.length;
    while (index-- > 0 && rowNos[index] != r);
    return index != -1;
  }
  
  
  
  // - -  I N S T A N C E   M E M B E R S  - -
  
  
  
  
  
  private final ArrayList<SourceRow> gatheredRows = new ArrayList<>();
  
  private final MessageDigest digest = SldgConstants.DIGEST.newDigest();
  
  private final Predicate<Long> rowSelector;
  
  private final SourceRowBuilder builder;
  
  
  
  /**
   * No-salt constructor.
   * 
   * @param rowSelector not {@code null}
   */
  public SourceRowGatherer(Predicate<Long> rowSelector) {
    this(rowSelector, new SourceRowBuilder());
  }
  
  
  /**
   * Salt-all constructor. Invokes {@code
   * this(rowSelector, new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt)) }
   * 
   * @param rowSelector not {@code null}
   * @param tableSalt   not {@code null}
   */
  public SourceRowGatherer(Predicate<Long> rowSelector, TableSalt tableSalt) {
    this(rowSelector, new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt));
  }
  
  
  /**
   * Full constructor. Invokes {@code
   * this(rowSelector, new SourceRowBuilder(saltScheme, tableSalt)) }
   * 
   * @param rowSelector not {@code null}
   * @param saltScheme  not {@code null}
   * @param tableSalt   not {@code null}, if {@code saltScheme.hasSalt()}
   */
  public SourceRowGatherer(
      Predicate<Long> rowSelector, SaltScheme saltScheme, TableSalt tableSalt) {
    
    this(rowSelector, new SourceRowBuilder(saltScheme, tableSalt));
  }

  /**
   * Full constructor.
   * 
   * @see #selectSet(Set)
   * @see #selectCollection(Collection)
   * @see #selectRange(long, long)
   * @see #selectRows(long...)
   */
  public SourceRowGatherer(
      Predicate<Long> rowSelector, SourceRowBuilder builder) {
    
    this.rowSelector = Objects.requireNonNull(rowSelector, "rowSelector");
    this.builder = Objects.requireNonNull(builder, "builder");
  }

  @Override
  public void lineOffsets(long offset, long lineNo) {   }

  @Override
  public void ledgeredLine(long rowNo, Grammar grammar, long offset, long lineNo, ByteBuffer line) {
    
    if (!rowSelector.test(rowNo))
      return;
    
    if (!gatheredRows.isEmpty() && gatheredRows.getLast().no() >= rowNo)
      throw new IllegalStateException(
          "out-of-sequence rowNo %d; last was %d [%d]"
          .formatted(
              rowNo, gatheredRows.getLast().no(), gatheredRows.size()-1));
    
    gatheredRows.add(
        SourceRowLogBuilder.buildRow(rowNo, grammar, builder, digest, line));
//    var tokens = grammar.parseTokens(Strings.utf8String(line));
//    if (tokens.isEmpty()) {
//      if (strict)
//        gatheredRows.add(SourceRow.nullRow(rowNo));
//      
//    } else {
//      SourceRow srcRow =
//          builder.buildRow(
//              rowNo,
//              Lists.repeatedList(DataType.STRING, tokens.size()),
//              tokens,
//              digest);
//
//      gatheredRows.add(srcRow);
//    }
  }

  @Override
  public void skippedLine(long offset, long lineNo, ByteBuffer line) {   }
  
 
  
  public List<SourceRow> gatheredRows() {
    return Collections.unmodifiableList(gatheredRows);
  }
  

}






