/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.src.TableSalt;

/**
 * Grammar (parsing rules) plus <em>secret</em> salt.
 * 
 * <h2>One-to-one salt with files</h2>
 * <p>
 * The same salt ought not be used with more than one file.
 * Otherwise, hash proofs about the contents of one file
 * will facilitate certain rainbow attacks on a second file
 * that's using the same salt.
 * </p>
 * 
 * <h2>Clearing the salt</h2>
 * <p>
 * Because the salt is sensitive information, methods are provided to
 * (help) clear it from memory.
 * </p>
 * @see #clearSalt()
 * @see #isSaltCleared()
 */
public class HashingGrammar implements Serial {
  
  /**
   * Generates and returns a new 32-byte, secure, random salt.
   */
  public static byte[] secureSalt() {
    byte[] salt = new byte[SldgConstants.HASH_WIDTH];
    new SecureRandom().nextBytes(salt);
    return salt;
  }
  
  private final ByteBuffer salt;
  private final Grammar grammar;
  
  
  /**
   * Creates an instance by generating a new secure random salt
   * byte sequence.
   * 
   * @param grammar not null
   * @see #secureSalt()
   */
  public HashingGrammar(Grammar grammar) {
    this(secureSalt(), grammar);
  }
  
  
  /**
   * Creates an instance.
   * 
   * @param salt    high entropy 32-byte random (do not modify!)
   * @param grammar not null
   */
  public HashingGrammar(byte[] salt, Grammar grammar) {
    this(ByteBuffer.wrap(salt), grammar, false);
  }

  /**
   * Creates an instance.
   * 
   * @param salt    high entropy 32-byte random (do not modify!)
   * @param grammar not null
   */
  public HashingGrammar(ByteBuffer salt, Grammar grammar) {
    this(salt.slice(), grammar, false);
  }
  
  private HashingGrammar(ByteBuffer salt, Grammar grammar, boolean ignored) {
    this.salt = salt;
    this.grammar = Objects.requireNonNull(grammar, "null grammar");
    if (salt.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("salt (byte len): " + salt);
  }
  
  
  
  /**
   * Returns a no-side-effects, state hasher.
   */
  public StateHasher stateHasher() {
    return new StateHasher(
        new TableSalt(salt()),
        grammar.commentPrefix().map(StateHasher::commentPrefixMatcher).orElse(null),
        grammar.delimiters());
  }
  
  
  /**
   * Returns a seal for the given {@code log} file.
   * The seal is not backed anywhere on the file system.
   * Ideally, there ought to be a one-to-one correspondence
   * between instances of this class and log files, since the
   * salting is not designed to be shared.
   * 
   * @param log
   * @return
   * @throws IOException
   */
  public Seal seal(File log) throws IOException {
    var state = stateHasher().play(log);
    return new Seal(state.rowNumber(), state.rowHash(), this);
  }
  
  
  /**
   * Returns a read-only view of the salt.
   * 
   * @throws IllegalStateException if {@code isSaltCleared()} returns {@code true}
   */
  public ByteBuffer salt() {
    var b = salt.asReadOnlyBuffer();
    checkNotCleared();
    return b;
  }
  
  /** @return not {@code null} */
  public Grammar grammar() {
    return grammar;
  }
  
  /**
   * Determines if the salt is cleared. If {@code true}, then all views of
   * {@linkplain #salt()} are invalidated (zeroed).
   * 
   * @see #clearSalt()
   */
  public boolean isSaltCleared() {
    return salt.remaining() != SldgConstants.HASH_WIDTH;
  }
  
  
  private void checkNotCleared() {
    if (isSaltCleared())
      throw new IllegalStateException("salt has been cleared");
  }
  
  /**
   * Clears (zeroes) the salt (if it's not read-only).
   * Invalidates all views of {@linkplain #salt()}.
   * 
   * @see #isSaltCleared()
   */
  public void clearSalt() {
    if (!salt.isReadOnly()) {
      while (salt.hasRemaining())
        salt.put((byte) 0);
    }
  }

  
  
  
  @Override
  public int serialSize() {
    return SldgConstants.HASH_WIDTH + grammar.serialSize();
  }

  /**
   * @throws IllegalStateException if {@code isSaltCleared()} returns {@code true}
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws IllegalArgumentException {
    out.put(salt());
    grammar.writeTo(out);
    return out;
  }
  
  
  
  
  
  /** Equality semantics based on both salt and grammar. */
  @Override
  public final boolean equals(Object o) {
    return o instanceof HashingGrammar h && h.salt.equals(salt) && h.grammar.equals(grammar);
  }

  /** Hash code based only on salt. */
  @Override
  public int hashCode() {
    return salt.hashCode();
  }
  
  
  /**
   * Loads and returns the serial representation of an instance.
   */
  public static HashingGrammar load(ByteBuffer in) throws BufferUnderflowException {
    byte[] salt = new byte[SldgConstants.HASH_WIDTH];
    in.get(salt);
    var grammar = Grammar.load(in);
    return new HashingGrammar(salt, grammar);
  }

}
