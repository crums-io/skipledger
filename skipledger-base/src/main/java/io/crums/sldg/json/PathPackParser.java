/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg.json;


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.PathPack;
import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class PathPackParser implements JsonEntityParser<PathPack> {
  
  private final HashEncoding hashCodec;
  private final String targetNosTag;
  private final String hashesTag;
  
  
  
  public PathPackParser(HashEncoding hashCodec, String targetNosTag, String hashesTag) {
    this.hashCodec = Objects.requireNonNull(hashCodec);
    this.targetNosTag = targetNosTag.trim();
    this.hashesTag = hashesTag.trim();
    
    boolean fail =
        targetNosTag.isEmpty() ||
        hashesTag.isEmpty() ||
        targetNosTag.equals(hashesTag);
    
    if (fail)
      throw new IllegalArgumentException(
          "illegal tags in args: " +
          Arrays.asList(hashCodec, targetNosTag, hashesTag));
  }
  

  @Override
  public JSONObject injectEntity(PathPack pack, JSONObject jObj) {
    var targetRns = pack.compressedRowNos();
    var jTargetNos = new JSONArray(targetRns.size());
    jTargetNos.addAll(targetRns);
    jObj.put(targetNosTag, jTargetNos);
    
    var refsBlock = pack.refsBlock();
    var inputsBlock = pack.inputsBlock();
    int initCapacity =
        (refsBlock.remaining() + inputsBlock.remaining())
        / SldgConstants.HASH_WIDTH;
    var jHashes = new JSONArray(initCapacity);
    appendHashes(jHashes, refsBlock);
    appendHashes(jHashes, inputsBlock);
    jObj.put(hashesTag, jHashes);
    
    return jObj;
  }
  
  private void appendHashes(JSONArray jHashes, ByteBuffer buffer) {
    while (buffer.hasRemaining())
      jHashes.add(
          hashCodec.encode(
              BufferUtils.slice(buffer, SldgConstants.HASH_WIDTH)));
    
  }
  

  @Override
  public PathPack toEntity(JSONObject jObj) throws JsonParsingException {
    var jTargets = JsonUtils.getJsonArray(jObj, targetNosTag, true);
    var jHashes =  JsonUtils.getJsonArray(jObj, hashesTag, true);
    try {
      
      List<Long> targetRns = Lists.map(
          JsonUtils.toNumbers(jTargets, true),
          n -> n instanceof Long lng ? lng : n.longValue());
      
      
      ByteBuffer hashBlock = ByteBuffer.allocate(
          jHashes.size() * SldgConstants.HASH_WIDTH);
      
      for (var s : jHashes)
        hashBlock.put(hashCodec.decode(s.toString()));
      
      assert !hashBlock.hasRemaining();
      hashBlock.flip();
      hashBlock = hashBlock.asReadOnlyBuffer();
      
      return PathPack.load(targetRns, hashBlock, true);

    } catch (JsonParsingException jpx) {
      throw jpx;
    } catch (Exception x) {
      throw new JsonParsingException("on parsing path pack: " + x.getMessage(), x);
    }
  }

}
