/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src.json;


import java.util.List;

import io.crums.sldg.salt.EpochedTableSalt;
import io.crums.sldg.salt.EpochedTableSalt.EpochSeed;
import io.crums.sldg.salt.TableSalt;
import io.crums.util.json.JsonEntityReader;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;


/**
 * Reads a {@link TableSalt} from the {@code /salts} JSON array response.
 *
 * <p>This is a thin wrapper over {@link EpochSeedParser}: it delegates list
 * parsing to {@link EpochSeedParser#PARSER} and passes the resulting
 * {@link EpochSeed} collection to
 * {@link EpochedTableSalt#createTableSalt(java.util.Collection)}.</p>
 *
 * <p>The primary entry point is {@link #toTableSalt(JSONArray)}, which handles
 * the multi-epoch {@code /salts} response directly.  The {@link JsonEntityReader}
 * implementation ({@link #toEntity(JSONObject)}) handles the degenerate
 * single-epoch case.</p>
 */
public class TableSaltReader implements JsonEntityReader<TableSalt> {

  public static final TableSaltReader READER = new TableSaltReader();


  /**
   * Parses the {@code /salts} JSON array into a {@link TableSalt}.
   *
   * @param arr JSON array of epoch-seed objects
   */
  public TableSalt toTableSalt(JSONArray arr) throws JsonParsingException {
    List<EpochSeed> seeds = EpochSeedParser.PARSER.toEntityList(arr);
    try {
      return EpochedTableSalt.createTableSalt(seeds);
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
  }


  /** Parses a single-epoch {@link TableSalt} from one epoch-seed JSON object. */
  @Override
  public TableSalt toEntity(JSONObject jObj) throws JsonParsingException {
    EpochSeed seed = EpochSeedParser.PARSER.toEntity(jObj);
    try {
      return EpochedTableSalt.createTableSalt(List.of(seed));
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
  }
}
