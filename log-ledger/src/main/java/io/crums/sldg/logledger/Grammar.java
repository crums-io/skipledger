/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.function.Predicate;

import io.crums.util.Strings;

/**
 * The log's grammar: comment-line filter, token delimiters, and
 * whether empty lines count as rows in the ledger. Instances are
 * supposed to be stateless (thread-safe), unless explicitly documented
 * otherwise.
 * <p>
 * Since instances are immutable, <em>mutator methods return new instances.</em>
 * </p>
 */
public class Grammar {
  
  
  
  /**
   * Constructs and returns an efficient prefix matcher. Used as a
   * comment predicate.
   * 
   * @param prefix      not empty
   */
  public static Predicate<ByteBuffer> prefixMatcher(String prefix) {
    if (prefix.isEmpty())
      throw new IllegalArgumentException("empty prefix");
    final byte[] p = Strings.utf8Bytes(prefix);
    if (p.length == 1) {
      final byte pb = p[0];
      return new Predicate<ByteBuffer>() {
        @Override
        public boolean test(ByteBuffer t) {
          int pos = t.position();
          return pos < t.limit() && t.get(pos) == pb;
        }
      };
    }
    
    return new Predicate<ByteBuffer>() {
      @Override
      public boolean test(ByteBuffer t) {
        int pos = t.position();
        int index = p.length;
        if (index > t.limit() - pos)
          return false;
        while (index-- > 0 && t.get(pos + index) == p[index]);
        return index == -1;
      }
    };
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  /** Default whitespace-delimited, empty lines included. */
  public final static Grammar DEFAULT = new Grammar();
  
  
  private final String tokenDelimiters;
  private final Predicate<ByteBuffer> commentMatcher;
  private final boolean skipBlankLines;
  
  
  
  
  
  /**
   * Constructs a whitespace-token-delimiting instance with no
   * comment-matcher. Blank lines are not skipped.
   */
  public Grammar() {
    this(null, null, false);
  }
  
  
  /**
   * Constructor defaults to blank-lines-skipped.
   * 
   * @param tokenDelimiters   as used in {@code StringTokenizer}; {@code null}
   *                          means whitespace-delimited
   * @param commentMatcher    optional (may be {@code null})
   */
  public Grammar(String tokenDelimiters, Predicate<ByteBuffer> commentMatcher) {
    this(tokenDelimiters, commentMatcher, true);
  }

  
  /**
   * Full constructor. (Default paramater values are {@code null} and
   * {@code false}.)
   * 
   * @param tokenDelimiters   as used in {@code StringTokenizer}; {@code null}
   *                          means whitespace-delimited
   * @param commentMatcher    optional (may be {@code null})
   * @param skipBlankLines    if {@code true}, then blank lines are skipped
   */
  public Grammar(
      String tokenDelimiters,
      Predicate<ByteBuffer> commentMatcher,
      boolean skipBlankLines) {
    
    this.tokenDelimiters = tokenDelimiters;
    this.commentMatcher = commentMatcher;
    this.skipBlankLines = skipBlankLines;
    
    if (tokenDelimiters != null) try {
      parseTokens("testing 1, 2 .. \f \r\n\f\n");
    } catch (Exception x) {
      throw new IllegalArgumentException(
          "tokenDelimiters (quoted): \"" + tokenDelimiters + "\"");
    }
  }
  
  
  /**
   * Returns an instance with the given comment-matcher.
   * 
   * @param matcher     may be set to {@code null}
   * @return    {@code this} if the instance already has the given matcher;
   *            a new instance, o.w.
   */
  public Grammar commentMatcher(Predicate<ByteBuffer> matcher) {
    return Objects.equals(this.commentMatcher, matcher) ?
        this :
          new Grammar(this.tokenDelimiters, matcher, this.skipBlankLines);
  }
  
  
  /** Returns the optional comment matcher. Commented-out lines are ignored. */
  public Optional<Predicate<ByteBuffer>> commentMatcher() {
    return Optional.ofNullable(commentMatcher);
  }
  
  
  
  /**
   * Returns an instance with the given token delimiters.
   * 
   * @param delimiters  {@code null} means whilespace delimited
   * @return   {@code this} if the instance already has the given token
   *            delimiters; a new instance, o.w.
   */
  public Grammar tokenDelimiters(String delimiters) {
    return Objects.equals(this.tokenDelimiters, delimiters) ?
        this :
          new Grammar(delimiters, this.commentMatcher, this.skipBlankLines);
  }
  
  /**
   * Returns the token delimiters, if any. If not present, then tokenization
   * is whitespace-delimited.
   */
  public Optional<String> tokenDelimiters() {
    return Optional.ofNullable(tokenDelimiters);
  }
  
  
  
  
  
  
  
  public List<String> parseTokens(String line) {
    if (line.isEmpty())
      return List.of();
    
    var tokenizer =
        tokenDelimiters == null ?
            new StringTokenizer(line) :
            new StringTokenizer(line, tokenDelimiters);
    
    List<String> out = new ArrayList<>();
    while (tokenizer.hasMoreTokens())
      out.add(tokenizer.nextToken());
    
    return out;
  }
  
  
  
  
  public Grammar skipBlankLines(boolean skipBlank) {
    return this.skipBlankLines == skipBlank ?
        this :
          new Grammar(this.tokenDelimiters, this.commentMatcher, skipBlank);
  }
  
  
  public boolean skipBlankLines() {
    return skipBlankLines;
  }
  
  
  

}














