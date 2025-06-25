/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;

import java.io.File;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.Optional;

import io.crums.io.SerialFormatException;
import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.SaltScheme;

/**
 * Parsing grammar and (optional, but recommended) salt, governing how
 * ledgerable lines are hashed.
 * 
 * <h2>Where Recorded</h2>
 * <p>
 * While the salt for each log file salt must be unique, a collection of
 * log files may share the same grammar. This class supports recording
 * both the log-file-specific salt and the grammar in a single file.
 * </p>
 * 
 * 
 */
public final class HashingRules {
  
  public final static HashingRules WS_NO_SALT = new HashingRules(null, null);
  
  private final static byte GRAM_FLAG = 0x1;
  private final static byte SALT_FLAG = 0x2;
  private final static byte ALL_FLAGS = GRAM_FLAG + SALT_FLAG;
  
  private final Grammar grammar;
  
  private final TableSalt salter;

  
  /**
   * Full constructor.
   * 
   * @param grammar     if {@code null}, then the default grammar is used
   * @param salter      if not {@code null}, then row tokens are salted.
   */
  public HashingRules(Grammar grammar, TableSalt salter) {
    this.grammar = grammar == null ? Grammar.DEFAULT : grammar;
    this.salter = salter;
  }
  
  
  /**
   * Returns the salt scheme. Either all cells are salted, or none are.
   */
  public SaltScheme saltScheme() {
    return salter == null ? SaltScheme.NO_SALT : SaltScheme.SALT_ALL;
  }
  
  
  /** Returns {@code true} if salt is used. */
  public boolean hasSalt() {
    return saltScheme().hasSalt();
  }
  
  
  
  public Grammar grammar() {
    return grammar;
  }
  
  /**
   * Returns the {@linkplain TableSalt}, if present.
   * @see #hasSalt()
   */
  public Optional<TableSalt> salter() {
    return Optional.ofNullable(salter);
  }
  
  
  

  public static HashingRules load(ByteBuffer in) {
    return load(in, null);
  }
  
  
  public static HashingRules load(ByteBuffer in, Grammar grammar) {
    final boolean hasGrammar, hasSalt;
    
    switch (in.get()) {
    case 0:         hasGrammar = hasSalt = false; break;
    case GRAM_FLAG: hasGrammar = true; hasSalt = false; break;
    case SALT_FLAG: hasGrammar = false; hasSalt = true; break;
    case ALL_FLAGS: hasGrammar = hasSalt = true; break;
    default:
      throw new SerialFormatException(
          "illegal flag (%d) in %s".formatted(in.get(in.position() - 1), in));
    }
    
    {
      Grammar gm = hasGrammar ?
          Files.SimpleGrammar.load(in).toGrammar() :
            Grammar.DEFAULT;
      
      if (grammar == null)
        grammar = gm;
      else if (hasGrammar)
        System.getLogger(LglConstants.LOG_NAME).log(
            Level.INFO, "overriding read grammar with " + grammar);
    }
    
    TableSalt salter = hasSalt ?
        new TableSalt(BufferUtils.slice(in, LglConstants.SEED_SIZE)) :
          null;
    
    
    return new HashingRules(grammar, salter);
  }
  
  

  public static HashingRules load(File file) {
    return load(file, null);
  }
  
  
  public static HashingRules load(File file, Grammar grammar) {
    return load( Files.loadSansHeader(file), grammar);
  }
  
  
  
  
  public static HashingRules initSalt(
      File file,
      Grammar grammar,
      byte[] salt) {
    
    checkSalt(salt);
    var out = ByteBuffer.allocate(salt.length + 1);
    out.put(SALT_FLAG).put(salt);
    Files.toFile(out.flip(), file);
    return load(file, grammar);
  }
  
  
  
  private static void checkSalt(byte[] salt) {
    if (salt.length != LglConstants.SEED_SIZE)
      throw new IllegalArgumentException(
          "expected %d salt bytes; actual was %d"
          .formatted(LglConstants.SEED_SIZE, salt.length));
  }
  
  public static HashingRules init(
      File rulesFile,
      boolean skipBlankLines,
      String tokenDelimiters,
      String commentPrefix,
      byte[] salt) {
    
    final boolean hasSalt = salt != null && salt.length != 0;
    if (hasSalt)
      checkSalt(salt);
    
    final boolean hasGrammar =
        !skipBlankLines ||
        (tokenDelimiters != null && !tokenDelimiters.isEmpty()) ||
        (commentPrefix != null && !commentPrefix.isEmpty());
    
    byte flags = 0;
    if (hasSalt)
      flags |= SALT_FLAG;
    if (hasGrammar)
      flags |= GRAM_FLAG;
    
    if (flags == 0)
      System.getLogger(LglConstants.LOG_NAME).log(
          Level.WARNING, "Writing default rules to " + rulesFile);
    
    
    Files.SimpleGrammar simp = hasGrammar ?
        new Files.SimpleGrammar(skipBlankLines, tokenDelimiters, commentPrefix) :
          null;
    
    int sizeEstimate = 1;
    if (hasSalt)
      sizeEstimate += LglConstants.SEED_SIZE;;
    if (hasGrammar)
      sizeEstimate += simp.estimateSize();
    
    ByteBuffer out = ByteBuffer.allocate(sizeEstimate);
    out.put(flags);
    if (hasGrammar)
      simp.writeTo(out);
    if (hasSalt)
      out.put(salt);
    
    Files.toFile(out.flip(), rulesFile);
    return load(rulesFile);
  }
  
  
  
  

}
