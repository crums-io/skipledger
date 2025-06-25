/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.nio.ByteBuffer;
import java.security.MessageDigest;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.src.DataType;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.SourceRowBuilder;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * Builds {@linkplain SourceRow}s from individual ledgered lines.
 * The set-up (grammar used to parse a line to token, the secret seet for
 * salting those tokens) and procedures to generate {@code SourceRow}s
 * from logs are centralized here.
 * 
 * <h2>Not Thread-safe</h2>
 * <p>
 * Instances must only be accessed from a single thread. (The work-digest
 * each instance uses is state-ful.)
 * </p>
 */
public class SourceRowLogBuilder {
  
  
  static SourceRow buildRow(
      long rowNo, Grammar grammar, SourceRowBuilder builder,
      MessageDigest digest, ByteBuffer line) {
    
    return buildRow(rowNo, grammar, builder, digest, Strings.utf8String(line));
  }
  
  static SourceRow buildRow(
      long rowNo, Grammar grammar, SourceRowBuilder builder,
      MessageDigest digest, String line) {
    
    var tokens = grammar.parseTokens(line);
    return
        tokens.isEmpty() ?
            SourceRow.nullRow(rowNo) :
              builder.buildRow(
                  rowNo,
                  Lists.repeatedList(DataType.STRING, tokens.size()),
                  tokens,
                  digest);
  }
  
  private final MessageDigest digest = SldgConstants.DIGEST.newDigest();
  
  private final HashingRules rules;
  
  private SourceRowBuilder builder;

  /**
   * Creates an instance with the given hashing rules.
   */
  public SourceRowLogBuilder(HashingRules rules) {
    this.rules = rules;
    this.builder = rules.hasSalt() ?
        new SourceRowBuilder(rules.saltScheme(), rules.salter().get()) :
          new SourceRowBuilder();
  }
  
  
  /** Returns the hashing rules this instance uses. */
  public HashingRules rules() {
    return rules;
  }
  
  
  /** Builds and returns a {@linkplain SourceRow} using the given line and row no. */
  public SourceRow buildRow(long rowNo, ByteBuffer line) {
    return buildRow(rowNo, Strings.utf8String(line));
  }
  

  /** Builds and returns a {@linkplain SourceRow} using the given line and row no. */
  public SourceRow buildRow(long rowNo, String line) {
    return buildRow(rowNo, rules.grammar(), builder, digest, line);
  }

}



