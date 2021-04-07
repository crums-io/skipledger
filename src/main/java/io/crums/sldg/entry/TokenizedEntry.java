/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.entry;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;


/**
 * Word tokenized hashing. Design to support word-by-word redactions in a single entry.
 *
 * @see #hash()
 */
public class TokenizedEntry extends TextEntry {
  
  private final static ByteBuffer NO_WORDS_HASH;
  
  static {
    byte[] emptyStringHash = new byte[SldgConstants.HASH_WIDTH];
    byte ff = (byte) -1;
    for (int index = 0; index < emptyStringHash.length; ++index)
      emptyStringHash[index] = ff;
    NO_WORDS_HASH = ByteBuffer.wrap(emptyStringHash).asReadOnlyBuffer();
  }
  
  
  public static ByteBuffer noWordsHash() {
    return NO_WORDS_HASH.asReadOnlyBuffer();
  }
  
  
  
  
  
  
  

  public TokenizedEntry(String text, long rowNumber) {
    super(text, rowNumber);
  }

  public TokenizedEntry(String text, EntryInfo info) {
    super(text, info);
  }
  
  /**
   * Promotion constructor.
   */
  public TokenizedEntry(TextEntry copy) {
    super(copy, copy.rowNumber());
  }

  public TokenizedEntry(TextEntry copy, long rowNumber) {
    super(copy, rowNumber);
  }

  public TokenizedEntry(ByteBuffer contents, long rowNumber) {
    super(contents, rowNumber);
  }

  public TokenizedEntry(ByteBuffer contents, EntryInfo info) {
    super(contents, info);
  }

  
  
  
  @Override
  public TokenizedEntry reNumber(long rowNumber) {
    return rowNumber == rowNumber() ? this : new TokenizedEntry(this, rowNumber);
  }

  /**
   * {@inheritDoc}
   * <h2>Word-redactable Hashing Scheme</h2>
   * <p>
   * First, a list of SHA-256 hashes is assembled from the individual words in the text.
   * If the list is empty the {@linkplain #noWordsHash()} is returned; if the list is a
   * singleton, then the hash of that single word is returned; otherwise, the hash of the
   * concatentation of the individual word hashes is returned.
   * </p>
   * <p>
   * TODO: (already on a tangent)
   * </p>
   */
  @Override
  public ByteBuffer hash() {
    MessageDigest digest = SldgConstants.DIGEST.newDigest();
    
    List<byte[]> wordHashes = wordHashes(digest);
    switch (wordHashes.size()) {
    case 0:   return noWordsHash();
    case 1:   return ByteBuffer.wrap(wordHashes.get(0));
    }
    
    digest.reset();
    for (byte[] wh : wordHashes)
      digest.update(wh);
    
    return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
  }
  
  
  /**
   * Returns the hash of the individual words, in order of occurance.
   * 
   * @return non-null, but possibly empty
   */
  public List<ByteBuffer> wordHashes() {
    MessageDigest digest = SldgConstants.DIGEST.newDigest();
    return Lists.map(wordHashes(digest), b -> ByteBuffer.wrap(b).asReadOnlyBuffer());
  }
  
  
  protected final List<byte[]> wordHashes(MessageDigest digest) {
    ArrayList<byte[]> wordHashes = new ArrayList<>();
    StringTokenizer tokenizer = newTokenizer(text());
    while (tokenizer.hasMoreTokens()) {
      byte[] word = toBytes(tokenizer.nextToken());
      digest.reset();
      wordHashes.add(digest.digest(word));
    }
    return wordHashes;
  }
  
  
  /**
   * Returns the word tokenizer. The default uses whitespace as a delimiter. I.e.
   * whitespace doesn't count.
   * @param text
   * @return
   */
  protected StringTokenizer newTokenizer(String text) {
    return new StringTokenizer(text);
  }

}
