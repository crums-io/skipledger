/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src.json;


import io.crums.sldg.src.SaltScheme;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;


/**
 * JSON parser/writer for {@link SaltScheme}.
 *
 * <h2>Wire format</h2>
 * <pre>{@code
 * {
 *   "salt_code":    <int>,    // 1 = positive (listed cells salted); 0 = negative (listed cells unsalted)
 *   "cell_indices": [<int>, ...]
 * }
 * }</pre>
 *
 * <p>Special cases: {@link SaltScheme#NO_SALT} and {@link SaltScheme#SALT_ALL} are
 * represented with an empty {@code cell_indices} array; they are distinguished by
 * {@code salt_code} (1 and 0 respectively).</p>
 */
public class SaltSchemeParser implements JsonEntityParser<SaltScheme> {

  public static final String SALT_CODE_KEY    = "salt_code";
  public static final String CELL_INDICES_KEY = "cell_indices";

  public static final SaltSchemeParser PARSER = new SaltSchemeParser();


  @Override
  public JSONObject injectEntity(SaltScheme scheme, JSONObject jObj) {
    jObj.put(SALT_CODE_KEY, (long) (scheme.isPositive() ? 1 : 0));
    var arr = new JSONArray();
    for (int i : scheme.cellIndices()) arr.add((long) i);
    jObj.put(CELL_INDICES_KEY, arr);
    return jObj;
  }


  @Override
  public SaltScheme toEntity(JSONObject jObj) throws JsonParsingException {
    int saltCode = JsonUtils.getNumber(jObj, SALT_CODE_KEY, true).intValue();
    if (saltCode != 0 && saltCode != 1)
      throw new JsonParsingException(
          "'" + SALT_CODE_KEY + "' must be 0 or 1, got: " + saltCode);
    JSONArray arr = (JSONArray) jObj.get(CELL_INDICES_KEY);
    if (arr == null)
      throw new JsonParsingException("missing '" + CELL_INDICES_KEY + "'");
    int[] indices = new int[arr.size()];
    try {
      for (int i = 0; i < indices.length; i++)
        indices[i] = ((Number) arr.get(i)).intValue();
      return SaltScheme.of(indices, saltCode == 1);
    } catch (JsonParsingException x) {
      throw x;
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
  }
}
