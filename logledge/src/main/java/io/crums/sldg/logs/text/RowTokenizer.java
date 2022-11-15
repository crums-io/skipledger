/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Row parser implementation using {@code StringTokenizer}.
 * Instances are thread-safe and do not block under concurrent access.
 */
public class RowTokenizer implements SourceRowParser {

  private final String delimiters;
  
  /**
   * Creates a white-space delimited instance.
   * Note the upshot is that a blank line counts for naught.
   */
  public RowTokenizer() {
    delimiters = null;
  }
  
  /**
   * Full constructor.
   * 
   * @param delimiters token delimiters. {@code null} or empty means white-space (the default constructor)
   */
  public RowTokenizer(String delimiters) {
    this.delimiters = delimiters != null && delimiters.isEmpty() ? null : delimiters;
  }
  
  
  

  @Override
  public List<String> apply(String row) {
    var tokenizer = delimiters == null ?
        new StringTokenizer(row) : new StringTokenizer(row, delimiters);
    if (!tokenizer.hasMoreTokens())
      return List.of();

    var tokens = new ArrayList<String>();
    do {
      tokens.add(tokenizer.nextToken());
    } while (tokenizer.hasMoreTokens());
    
    return tokens;
  }

}


