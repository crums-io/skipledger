/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.util.Objects;
import java.util.Optional;

/**
 * 
 */
public record Header(TableTemplate headerTable, Optional<BorderContent> headContent) {
  
  /**
   * 
   * @param headerTable   not null
   * @param headContent   not null
   * @see #Header(TableTemplate)
   */
  public Header {
    Objects.requireNonNull(headerTable, "null header table");
    Objects.requireNonNull(headContent, "null head content option");
  }
  
  public Header(TableTemplate headerTable) {
    this(headerTable, Optional.empty());
  }

}
