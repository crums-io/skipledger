/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.io.buffer.BufferUtils;
import io.crums.util.Strings;

/**
 * Ledger meta information, not necessarily validated.
 * 
 */
public abstract class LedgerInfo implements Serial {
  
  
  /**
   * Loads and returns an instance from serial form.
   * 
   * @param in
   * @return
   * @throws SerialFormatException if {@code in} is malformed
   */
  public static LedgerInfo load(ByteBuffer in) throws SerialFormatException {
    StdProps props = StdProps.load(in);
    return switch (props.type()) {
    case TIMECHAIN  -> TimechainInfo.load(props, in);
    case TABLE      -> TableInfo.load(props, in);
    case LOG        -> LogInfo.load(props, in);
    case BSTREAM    -> BStreamInfo.load(props, in);
    };
  }
  
  
  
  /**
   * Standard properties all ledgers share. 
   * 
   * @param type        not null
   * @param alias       locally unique name (trimmed)
   * @param uri         optional (may be {@code null})
   * @param desc        optional description ({@code null} or blank counts
   *                    for naught)
   */
  public record StdProps(LedgerType type, String alias, URI uri, String desc)
      implements Serial {
    
    public StdProps {
      Objects.requireNonNull(type, "null type");
      alias = alias.trim();
      if (alias.isEmpty())
        throw new IllegalArgumentException("blank or empty alias");
      
    }
    
    public StdProps alias(String newAlias) {
      newAlias = newAlias.trim();
      return
          alias.equals(newAlias) ?
              this :
                new StdProps(type, newAlias, uri, desc);
    }
    
    public StdProps uri(URI newUri) {
      return
          Objects.equals(uri, newUri) ?
              this :
                new StdProps(type, alias, newUri, desc);
    }
    
    
    public StdProps desc(String newDesc) {
      return
          Objects.equals(desc, newDesc) ?
              this :
                new StdProps(type, alias, uri, newDesc);
    }

    @Override
    public int serialSize() {
      int tally = 4 + strlen(alias);
      if (uri == null)
        tally += 4;
      else
        tally += strlen(uri.toString());
      tally += strlen(desc);
      return tally;
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
      out.putInt(type().ordinal());
      putString(alias, out);
      if (uri == null)
        out.putInt(0);
      else
        putString(uri.toString(), out);
      putString(desc, out);
      return out;
    }
    
    
    public static StdProps load(ByteBuffer in) {
      try {
        int ordinal = in.getInt();
        var type = LedgerType.forOrdinal(ordinal);
        var alias = loadString(in);
        URI uri;
        {
          var strUri = loadString(in);
          uri = strUri == null ? null : new URI(strUri);
        }
        var desc = loadString(in);
        
        return new StdProps(type, alias, uri, desc);
      
      } catch (SerialFormatException sfx) {
        throw sfx;
      
      } catch (Exception x) {
        throw new SerialFormatException(
            "on loading StdProps from " + in + " -- detail: " + x, x);
      }
    }
  }
  
  
  
  
  static int strlen(String s) {
    return s == null || s.isBlank() ? 4 : Strings.utf8Bytes(s).length + 4;
  }
  
  
  static void putString(String s, ByteBuffer out) {
    if (s == null || s.isBlank()) {
      out.putInt(0);
    } else {
      byte[] b = Strings.utf8Bytes(s);
      out.putInt(b.length).put(b);
    }
  }
  
  /**
   * Loads a string, first reading its byte-length.
   * 
   * @return empty strings are returned as {@code null}
   */
  static String loadString(ByteBuffer in) {
    int len = in.getInt();
    if (len == 0)
      return null;
    if (len < 0)
      throw new SerialFormatException(
          "read negative string len %d at pos %d in %s"
          .formatted(len, in.position() - 4, in));
    
    if (len > in.remaining())
      throw new SerialFormatException(
          "read string len %d which will cause an underflow in %s"
          .formatted(len, in));
    
    return Strings.utf8String(BufferUtils.slice(in, len));
  }
  
  protected final StdProps props;

 
  
  LedgerInfo(StdProps props) {
    this.props = Objects.requireNonNull(props);
  }
  
  
  /**
   * Returns {@code true} iff the implied transformation would be valid.
   * The base implementation only requires the ledger {@linkplain #type()}
   * remain the same.
   * 
   * @see #verifyEdit(LedgerInfo)
   */
  public boolean isValidEdit(LedgerInfo newInfo) {
    return newInfo.type() == type();
  }
  
  /**
   * Throws a wrench if the given argument is an illegal edit.
   * 
   * @see #isValidEdit(LedgerInfo)
   */
  public void verifyEdit(LedgerInfo newInfo) throws IllegalArgumentException {
    if (!isValidEdit(newInfo))
      throw new IllegalArgumentException(
          "attempt to change type (from %s to %s)"
          .formatted(type(), newInfo.type()));
  }
  
  
  
  
  
  abstract LedgerInfo edit(StdProps props);
  
  private LedgerInfo editIfChanged(StdProps props) {
    return this.props.equals(props) ? this : edit(props);
  }
  
  /**
   * Returns the ledger type.
   * 
   * @return not null
   */
  public final LedgerType type() {
    return props.type;
  }
  
  /**
   * Returns the locally unique name for this ledger.
   * 
   * @return neither empty or nor null
   */
  public final String alias() {
    return props.alias;
  }
  
  
  public LedgerInfo alias(String newAlias) {
    return editIfChanged(props.alias(newAlias));
  }
  
  public Optional<URI> uri() {
    return Optional.ofNullable(props.uri);
  }
  
  public LedgerInfo uri(URI newUri) {
    return editIfChanged(props.uri(newUri));
  }
  
  public Optional<String> description() {
    return nonBlankOpt(props.desc);
  }
  
  public LedgerInfo description(String newDesc) {
    return editIfChanged(props.desc(newDesc));
  }
  
  /**
   * Instances are equal if they have the same properties.
   * 
   * @see #otherProperties()
   */
  public final boolean equals(Object o) {
    return o == this ||
        o instanceof LedgerInfo info &&
        info.props.equals(props) &&
        Objects.equals(otherProperties(), info.otherProperties());
  }
  
  /** Consistent with {@linkplain #equals(Object)}. */
  @Override
  public final int hashCode() {
    return props.alias.hashCode();
  }
  
  /**
   * Returns additional properties considered for equality evaluation.
   * May return {@code null}.
   */
  protected Object otherProperties() {
    return null;
  }
  
  
  
  static Optional<String> nonBlankOpt(String value) {
    return value != null && !value.isBlank() ? Optional.of(value) : Optional.empty();
  }

}







