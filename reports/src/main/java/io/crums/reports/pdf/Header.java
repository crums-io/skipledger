/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf;

import java.util.Objects;
import java.util.Optional;

/**
 * 
 */
public final class Header {
  
  private final BorderContent headContent;
  
  private final FixedTable headerTable;

  /**
   * 
   */
  public Header(FixedTable headerTable, BorderContent headContent) {
    this.headContent = headContent;
    this.headerTable = Objects.requireNonNull(headerTable, "null headerTable");
  }
  
  
  
  public Optional<BorderContent> getHeadContent() {
    return Optional.ofNullable(headContent);
  }
  
  
  public FixedTable getHeaderTable() {
    return headerTable;
  }
  
  
  
  
  public boolean equals(Object o) {
    return o == this ||
        (o instanceof Header other) &&
        FixedTable.equal(headerTable, other.headerTable) &&
        Objects.equals(headContent,  other.headContent);
  }
  
  
  public int hashCode() {
    int cHash = headContent == null ? -1 : headContent.hashCode();
    int tHash = headerTable.getCellCount() ^ headerTable.getFixedCells().hashCode();
    return cHash + 499 * tHash;
  }

}