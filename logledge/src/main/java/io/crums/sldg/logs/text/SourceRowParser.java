/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.util.List;
import java.util.function.Function;

/**
 * Interface for tokenizing a row of log data. The only reason to tokenize
 * such data is if you later wish to redact certain words or tokens when proving
 * the contents of the row. Since that is a common use case, it's assumed to be
 * the default.
 * 
 * @see #NOOP
 * @see #apply(String)
 */
public interface SourceRowParser extends Function<String, List<String>> {
  
  /** Noop instance. Returns the row as-is in a singleton {@code List}. */
  public final static SourceRowParser NOOP = new SourceRowParser() {
    @Override
    public List<String> apply(String row) {
      return List.of(row);
    }
  };

  /**
   * Returns a tokenized version of the row, if successful. Otherwise, an
   * <em>empty</em> return value signifies that the input does not count as
   * a row.
   * <p>
   * Note this is also an opportunity to <em>normalize</em> the row data,
   * even if you don't tokenize it.
   * </p>
   */
  @Override
  public List<String> apply(String row);

}
