/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Filters rows before parsing. For comments, for example.
 */
public class FilteredRowParser implements SourceRowParser {
  
  private final Predicate<String> filter;
  private final SourceRowParser base;

  
  
  public FilteredRowParser(Predicate<String> filter, SourceRowParser base) {
    this.filter = Objects.requireNonNull(filter, "null filter");
    this.base = Objects.requireNonNull(base, "null base parser");
  }

  @Override
  public List<String> apply(String row) {
    return filter.test(row) ? base.apply(row) : List.of();
  }

}
