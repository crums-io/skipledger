/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.fs;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Objects;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.src.TableSalt;

/**
 * 
 */
public class TableSaltFile extends TableSalt {

  
  public static TableSaltFile createInstance(File seedFile, byte[] seed) {
    Objects.requireNonNull(seedFile, "null table salt seed file");
    if (seedFile.exists())
      throw new IllegalArgumentException("seed file already exists: " + seedFile);
    Objects.requireNonNull(seed, "null table salt seed");
    if (seed.length != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("illegal seed length: " + seed.length);
    FileUtils.writeNewFile(seedFile, ByteBuffer.wrap(seed));
    
    for (int index = seed.length; index-- > 0;)
      seed[index] = 0;

    return new TableSaltFile(seedFile);
  }
  
  
  public static TableSaltFile loadInstance(File seedFile, Opening opening) {
    Objects.requireNonNull(seedFile, "null table salt seed file");
    Objects.requireNonNull(opening, "null opening");
    
    switch (opening) {
    case CREATE_ON_DEMAND:
      if (seedFile.exists())
        break;
    case CREATE:
      var random = new SecureRandom();
      byte[] seed = new byte[SldgConstants.HASH_WIDTH];
      random.nextBytes(seed);
      return createInstance(seedFile, seed);
    default:
    }
    
    if (!seedFile.isFile())
      throw new IllegalArgumentException("path to table seed not a file: " + seedFile);
    
    if (seedFile.length() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException(
          "expected " + SldgConstants.HASH_WIDTH + " bytes in table seed file " +
          seedFile + " Actual is " + seedFile.length() + " bytes");
    
    return new TableSaltFile(seedFile);
  }
  
  
  
  private TableSaltFile(File seedFile) {
    super(FileUtils.loadFileToMemory(seedFile));
  }

}
