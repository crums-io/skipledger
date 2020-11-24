/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.hash;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Specifies a hashing method. This class will probably find a home in another
 * module, as it would be useful across a range of projects.
 */
public interface Digest {
  
  
  public static boolean compatible(Digest digest, MessageDigest md) {
    Objects.requireNonNull(digest, "null digest");
    Objects.requireNonNull(md, "null md");
    return
        md.getDigestLength() == digest.hashWidth() &&
        md.getAlgorithm().equals(digest.hashAlgo());
  }
  
  public static boolean equal(Digest a, Digest b) {
    Objects.requireNonNull(a, "null a");
    Objects.requireNonNull(b, "null b");
    return a.hashWidth() == b.hashWidth() && a.hashAlgo().equals(b.hashAlgo());
  }
  
  
  
  int hashWidth();
  
  
  String hashAlgo();
  
  
  default MessageDigest newDigest() {
    String algo = hashAlgo();
    try {
      
      MessageDigest digest = MessageDigest.getInstance(algo);
      assert digest.getDigestLength() == hashWidth();
      
      return digest;
      
    } catch (NoSuchAlgorithmException nsax) {
      throw new RuntimeException("on creating digest with algo " + algo, nsax);
    }
  }
  
  

}
