/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src.json;


import io.crums.sldg.salt.EpochedTableSalt.EpochSeed;
import io.crums.util.Base64_32;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;


/**
 * JSON parser/writer for {@link EpochSeed}.
 *
 * <h2>Wire format</h2>
 * <pre>{@code
 * {
 *   "seedStart": <long>,
 *   "seed":      "<43-char Base64_32>"
 * }
 * }</pre>
 *
 * <p>Only Base64_32 encoding is supported: epoch seeds are sensitive values and
 * hex exposure is intentionally not offered.</p>
 */
public class EpochSeedParser implements JsonEntityParser<EpochSeed> {

  public static final String SEED_START_KEY = "seedStart";
  public static final String SEED_KEY       = "seed";

  public static final EpochSeedParser PARSER = new EpochSeedParser();


  @Override
  public JSONObject injectEntity(EpochSeed seed, JSONObject jObj) {
    jObj.put(SEED_START_KEY, seed.startRow());
    jObj.put(SEED_KEY, Base64_32.encode(seed.seed()));
    return jObj;
  }


  @Override
  public EpochSeed toEntity(JSONObject jObj) throws JsonParsingException {
    long   seedStart = JsonUtils.getNumber(jObj, SEED_START_KEY, true).longValue();
    String seedStr   = JsonUtils.getString(jObj, SEED_KEY, true);
    if (seedStr.length() != 43)
      throw new JsonParsingException(
          "'" + SEED_KEY + "' must be 43 Base64_32 chars; got length " + seedStr.length());
    try {
      return new EpochSeed(seedStart, Base64_32.decode(seedStr));
    } catch (JsonParsingException x) {
      throw x;
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
  }
}
