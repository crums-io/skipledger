/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.SldgConstants.DIGEST;
import static io.crums.sldg.SldgConstants.HASH_WIDTH;
import static io.crums.util.hash.Digest.bufferDigest;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.SldgConstants;
import io.crums.util.IntegralStrings;
import io.crums.util.Strings;

/**
 * 
 */
public abstract class ColumnValue implements Serial {
  
  
  
  public static ColumnValue newInstance(Object obj) {
    if (obj == null) {
      return NULL_VALUE;
    
    } else if (obj instanceof byte[]) {
      ByteBuffer bytes = ByteBuffer.wrap((byte[]) obj);
      return bytes.remaining() == HASH_WIDTH ? new HashValue(bytes) : new BytesValue(bytes);
    
    } else if (obj instanceof ByteBuffer) {
      ByteBuffer bytes = (ByteBuffer) obj;
      return bytes.remaining() == HASH_WIDTH ? new HashValue(bytes) : new BytesValue(bytes);
    
    } else if (obj instanceof CharSequence) {
      return new StringValue(obj.toString());
    
    } else if (obj instanceof Number) {
      return new NumberValue((Number) obj);
    
    } else
      throw new IllegalArgumentException("unrecognized object: " + obj);
  }
  
  
  public static ColumnValue loadValue(ByteBuffer in) {
    byte type = in.get(in.position());
    // this double switching is hopefully jitted out under load
    switch (ColumnType.forCode(type)) {
    case NULL:    in.get(); return NULL_VALUE;
    case HASH:    return HashValue.load(in);
    case BYTES:   return BytesValue.load(in);
    case STRING:  return StringValue.load(in);
    case NUMBER:  return NumberValue.load(in);
    default:
      throw new RuntimeException("unaccounted type " + type);
    }
  }
  
  
  
  
  private final ColumnType type;

  /**
   * 
   */
  private ColumnValue(ColumnType type) {
    this.type = Objects.requireNonNull(type, "null type");
  }
  
  
  /**
   * Returns the column type for this column value.
   */
  public ColumnType getType() {
    return type;
  }
  

  
  
  
  public ByteBuffer getHash() {
    return getHash(DIGEST.newDigest());
  }
  
  public abstract ByteBuffer getHash(MessageDigest digest);
  

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder(64).append(type.symbol()).append('[');
    appendValue(s);
    return s.append(']').toString();
  }
  
  
  
  
  protected abstract void appendValue(StringBuilder s);




  /**
   * All integral values are expressed as 8-byte {@code long}s.
   */
  public static class NumberValue extends ColumnValue {
    
    
    static NumberValue load(ByteBuffer in) {
      byte type = in.get();
      assert type == ColumnType.NUMBER.code();
      return new NumberValue(in.getLong());
    }
    
    private final long number;
    
    
    public NumberValue(Number number) {
      this(number.longValue());
    }
    
    public NumberValue(long number) {
      super(ColumnType.NUMBER);
      this.number = number;
    }
    
    
    
    
    public long getNumber() {
      return number;
    }
    

    @Override
    public ByteBuffer getHash(MessageDigest digest) {
      ByteBuffer lngBuff = ByteBuffer.allocate(8).putLong(number).flip();
      digest.reset();
      digest.update(lngBuff);
      return bufferDigest(digest);
    }




    @Override
    public int serialSize() {
      return 9;
    }




    @Override
    public ByteBuffer writeTo(ByteBuffer out) {
      return out.put(getType().code()).putLong(number);
    }
    
    @Override
    protected void appendValue(StringBuilder s) {
      s.append(number);
    }

    
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  public static class StringValue extends ColumnValue {
    
    static StringValue load(ByteBuffer in) {
      byte type = in.get();
      assert type == ColumnType.STRING.code();
      int len = 0xffff & in.getShort();
      byte[] bytes = new byte[len];
      in.get(bytes);
      return new StringValue(new String(bytes, Strings.UTF_8));
    }
    
    private final String string;
    private final byte[] bytes;
    
    
    public StringValue(String string) {
      super(ColumnType.STRING);
      this.string = Objects.requireNonNull(string, "null string argument");
      this.bytes = string.isEmpty() ? new byte[0] : string.getBytes(Strings.UTF_8);
      if (bytes.length > 0xffff)
        throw new IllegalArgumentException(
            "string byte-length greater than capacity (64k): " + bytes.length);
    }
    
    
    private StringValue(String string, byte[] bytes) {
      super(ColumnType.STRING);
      this.string = string;
      this.bytes = bytes;
    }
    
    
    public String getString() {
      return string;
    }
    

    @Override
    public ByteBuffer getHash(MessageDigest digest) {
      ByteBuffer intBuff = ByteBuffer.allocate(4).putInt(bytes.length).flip();
      digest.reset();
      digest.update(intBuff);
      digest.update(bytes);
      return bufferDigest(digest);
    }


    @Override
    public int serialSize() {
      return 3 + bytes.length;
    }


    @Override
    public ByteBuffer writeTo(ByteBuffer out) {
      return out.put(getType().code()).putShort((short) bytes.length).put(bytes);
    }


    @Override
    protected void appendValue(StringBuilder s) {
      s.append(string);
    }
    

  }
  
  
  public static int MAX_TO_STRING_CHARS = 480;
  
  
  public static class BytesValue extends ColumnValue {
    
    static BytesValue load(ByteBuffer in) {
      byte type = in.get();
      assert type == ColumnType.BYTES.code();
      int len = 0xffff & in.getShort();
      ByteBuffer bytes = len == 0 ? BufferUtils.NULL_BUFFER : BufferUtils.slice(in, len);
      return new BytesValue(bytes);
    }
    
    private final ByteBuffer bytes;
    
    public BytesValue(ByteBuffer bytes) {
      this(bytes, ColumnType.BYTES);
    }
    
    BytesValue(ByteBuffer bytes, ColumnType type) {
      super(type);
      this.bytes = BufferUtils.readOnlySlice(bytes);
      if (bytes.remaining() > 0xffff)
        throw new IllegalArgumentException(
            "string byte-length greater than capacity (64k): " + bytes);
    }
    
    
    public ByteBuffer getBytes() {
      return bytes.asReadOnlyBuffer();
    }
    
    
    public int size() {
      return bytes.remaining();
    }


    @Override
    public ByteBuffer getHash(MessageDigest digest) {
      int count = bytes.remaining();
      ByteBuffer intBuff = ByteBuffer.allocate(4).putInt(count).flip();
      digest.reset();
      digest.update(intBuff);
      digest.update(getBytes());
      return bufferDigest(digest);
    }

    @Override
    public int serialSize() {
      return 3 + size();
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) {
      return out.put(getType().code()).putShort((short) size()).put(getBytes());
    }

    @Override
    protected void appendValue(StringBuilder s) {
      ByteBuffer b = getBytes();
      if (bytes.remaining() > MAX_TO_STRING_CHARS / 2)
        b.limit(-1 + MAX_TO_STRING_CHARS / 2);
      IntegralStrings.appendHex(b, s).append("..");
    }
    
  }
  
  
  public static class HashValue extends BytesValue {

    static HashValue load(ByteBuffer in) {
      byte type = in.get();
      assert type == ColumnType.HASH.code();
      ByteBuffer bytes = BufferUtils.slice(in, SldgConstants.HASH_WIDTH);
      return new HashValue(bytes);
    }
    
    @Override
    public int serialSize() {
      return 1 + SldgConstants.HASH_WIDTH;
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) {
      return out.put(getType().code()).put(getBytes());
    }

    public HashValue(ByteBuffer bytes) {
      super(bytes, ColumnType.HASH);
      if (size() != HASH_WIDTH)
        throw new IllegalArgumentException("bytes not expected hash-width: " + bytes);
    }
    
    
    @Override
    public ByteBuffer getHash(MessageDigest digest) {
      return getBytes();
    }
    
  }
  
  
  /**
   * 
   */
  public final static ColumnValue NULL_VALUE =
      new ColumnValue(ColumnType.NULL) {
        
        @Override
        public ByteBuffer getHash(MessageDigest digest) {
          return DIGEST.sentinelHash();
        }

        @Override
        public int serialSize() {
          return 1;
        }

        @Override
        public ByteBuffer writeTo(ByteBuffer out) {
          return out.put(getType().code());
        }
        

        @Override
        public String toString() {
          return getType().symbol();
        }

        @Override
        protected void appendValue(StringBuilder s) {
        }
      };

}
