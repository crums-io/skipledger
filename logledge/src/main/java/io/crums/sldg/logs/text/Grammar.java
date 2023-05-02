/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

import io.crums.io.Serial;
import io.crums.util.Strings;


/**
 * Line tokenization and comment-line prefix settings.
 * 
 * @param delimiters  token char delimiters; {@code null} or empt means whitespace chars.
 *                    Duplicate characters are removed.
 * @param cPrefix     comment-line prefix; {@code null} or empty means no comment-lines.
 * 
 * @see #tokenDelimiters()
 * @see #commentPrefix()
 */
public record Grammar(String delimiters, String cPrefix) implements Serial {
  
  /**
   * The default grammar is whitespace-delimited tokenization with no comment-lines.
   */
  public final static Grammar DEFAULT = new Grammar(null, null);
  
  public Grammar {
    
    if (delimiters != null) {
      if (delimiters.isEmpty())
        delimiters = null;
      else {
        if (Strings.utf8Bytes(delimiters).length > 127)
          throw new IllegalArgumentException(
              "delimiters exceed maximum length (127): \"" + delimiters + "\"");
        
        var set = new TreeSet<Character>();
        for (int index = 0; index < delimiters.length(); ++index)
          set.add(delimiters.charAt(index));
        
        if (set.size() < delimiters.length()) {
          var del = new StringBuilder();
          set.forEach(del::append);
          delimiters = del.toString();
        }
      }
    }
    
    if (cPrefix != null) {
      if (cPrefix.isEmpty())
        cPrefix = null;
      else if (Strings.utf8Bytes(cPrefix).length > 0xff)
        throw new IllegalArgumentException(
            "cPrefix exceed maximum length (255): \"" + cPrefix + "\"");
      else if (cPrefix.contains("\n"))
        throw new IllegalArgumentException(
            "new-line ('\\n') in comment-prefix (quoted) \"" + cPrefix + "\"");
    }
  }
  
  
  /** Returns the token delimiter chars; empty means whitespace. */
  public Optional<String> tokenDelimiters() {
    return Optional.ofNullable(delimiters);
  }

  /** Returns the comment-line prefix; empty means no comment-lines. */
  public Optional<String> commentPrefix() {
    return Optional.ofNullable(cPrefix);
  }
  
  
  /** Determines whether this is the default grammar. */
  public boolean isDefault() {
    return delimiters == null && cPrefix == null;
  }
  
  
  /**
   * Returns a possibly mutated version.
   * 
   * @param delimiters  token char delimiters; {@code null} means whitespace chars.
   *                    Duplicate characters are removed.
   */
  public Grammar delimiters(String delimiters) {
    if (delimiters != null && delimiters.isEmpty())
      delimiters = null;
    if (Objects.equals(this.delimiters, delimiters))
      return this;
    return new Grammar(delimiters, cPrefix);
  }
  
  /**
   * Returns a possibly mutated version.
   * 
   * @param cPrefix     comment-line prefix; {@code null} means no comment-lines.
   */
  public Grammar cPrefix(String cPrefix) {
    if (cPrefix != null && cPrefix.isEmpty())
      cPrefix = null;
    if (Objects.equals(this.cPrefix, cPrefix))
      return this;
    return new Grammar(delimiters, cPrefix);
  }
  
  
  
  
  
  @Override
  public int serialSize() {
    return 2 + toBytes(delimiters).length + toBytes(cPrefix).length;
  }
  
  private byte[] toBytes(String value) {
    return value == null ? NULL : Strings.utf8Bytes(value);
  }
  private final static byte[] NULL = { };


  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    byte[] c = toBytes(cPrefix);
    byte[] d = toBytes(delimiters);
    out.put((byte) c.length).put((byte) d.length).put(c).put(d);
    return out;
  }
  
  
  
  /**
   * Loads and returns an instance's serial representation.
   * 
   * @throws IllegalArgumentException if {@code in} is malformed
   */
  public static Grammar load(ByteBuffer in) throws BufferUnderflowException {
    int cLen = 0xff & in.get();
    int dLen = 0xff & in.get();
    String cPrefix = getString(in, cLen);
    String delimiters = getString(in, dLen);
    return new Grammar(delimiters, cPrefix);
  }
  
  
  private static String getString(ByteBuffer in, int len) {
    if (len == 0)
      return null;
    byte[] ub = new byte[len];
    in.get(ub);
    return new String(ub, Strings.UTF_8);
  }
  
  

}
