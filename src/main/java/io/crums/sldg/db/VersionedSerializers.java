/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;

import io.crums.sldg.Nugget;
import io.crums.sldg.Path;
import io.crums.sldg.json.NuggetParser;
import io.crums.sldg.json.PathParser;

/**
 * 
 */
public class VersionedSerializers {
  
  // static members only
  private VersionedSerializers() {  }
  
  
  
  public final static EntitySerializer<Path> PATH_SERIALIZER =
      new VersionedEntitySerializer<>(PathParser.INSTANCE, Path::loadUnchecked);

  
  
  
  public final static EntitySerializer<Nugget> NUGGET_SERIALIZER =
      new VersionedEntitySerializer<>(NuggetParser.INSTANCE, Nugget::loadUnchecked);
  
  

}
