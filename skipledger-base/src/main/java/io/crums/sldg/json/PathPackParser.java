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
 * {@code PathPack} JSON parser.
 */
public class PathPackParser implements JsonEntityParser<PathPack> {
  
  private final HashEncoding hashCodec;
  private final String targetNosTag;
  private final String typeTag;
  private final String hashesTag;
  
  
  
  public PathPackParser(
      HashEncoding hashCodec, String targetNosTag,
      String typeTag, String hashesTag) {
    this.hashCodec = Objects.requireNonNull(hashCodec);
    this.targetNosTag = targetNosTag.trim();
    this.typeTag = typeTag.trim();
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
    var targetRns = pack.preStitchRowNos();
    var jTargetNos = new JSONArray(targetRns.size());
    jTargetNos.addAll(targetRns);
    jObj.put(targetNosTag, jTargetNos);
    
    jObj.put(typeTag, pack.isCondensed() ? PathPack.CNDN_TYPE : PathPack.FULL_TYPE);
    var refsBlock = pack.refsBlock();
    var inputsBlock = pack.inputsBlock();
    var funnelsBlock = pack.funnelsBlock();
    int initCapacity =
        (refsBlock.remaining() + inputsBlock.remaining() + funnelsBlock.remaining())
        / SldgConstants.HASH_WIDTH;
    var jHashes = new JSONArray(initCapacity);
    appendHashes(jHashes, inputsBlock);
    appendHashes(jHashes, funnelsBlock);
    appendHashes(jHashes, refsBlock);
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
    boolean condensed;
    {
      int type = JsonUtils.getInt(jObj, typeTag);
      condensed = switch (type) {
        case PathPack.FULL_TYPE   -> false;
        case PathPack.CNDN_TYPE   -> true;
        default                   ->
            throw new JsonParsingException(
                "unkown '" + typeTag + "' value " + type);
      };
    }
    var jHashes =  JsonUtils.getJsonArray(jObj, hashesTag, true);
    try {
      
      List<Long> targetRns = Lists.map(
          JsonUtils.toNumbers(jTargets, true),
          n -> n instanceof Long lng ? lng : n.longValue());
      
      
      ByteBuffer hashBlock = ByteBuffer.allocate(
          jHashes.size() * SldgConstants.HASH_WIDTH + 1);
      
      hashBlock.put((byte) (condensed ? 1 : 0));
      for (var s : jHashes)
        hashBlock.put(hashCodec.decode(s.toString()));
      
      assert !hashBlock.hasRemaining();
      hashBlock.flip();
      hashBlock = hashBlock.asReadOnlyBuffer();
      
      var pack = PathPack.load(targetRns, hashBlock);
      
      if (hashBlock.hasRemaining())
        throw new JsonParsingException(
            "too many hashes: " +
            (hashBlock.remaining() / SldgConstants.HASH_WIDTH) + " unread");

      return pack;

    } catch (JsonParsingException jpx) {
      throw jpx;
    } catch (Exception x) {
      throw new JsonParsingException("on parsing path pack: " + x.getMessage(), x);
    }
  }

}







