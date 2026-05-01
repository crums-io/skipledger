/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src.json;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;

import io.crums.sldg.src.Cell;
import io.crums.sldg.src.DataType;
import io.crums.sldg.src.SharedConstants;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Base64_32;
import io.crums.util.IntegralStrings;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;


/**
 * JSON parser/writer for {@link SourceRow}.
 *
 * <h2>Row format</h2>
 * <pre>{@code
 * {
 *   "row":   <long>,
 *   "salt":  "<base64 or hex>",   // optional; row-level salt (32 bytes)
 *   "cells": [ <cell>, ... ]
 * }
 * }</pre>
 *
 * <h2>Cell forms</h2>
 * <p>
 * Cells may be written compactly (bare JSON value) when no individual cell
 * salt is needed and the type is unambiguously inferrable from the JSON type:
 * </p>
 * <ul>
 *   <li>{@code null} → {@code NULL}</li>
 *   <li>{@code true} / {@code false} → {@code BOOL}</li>
 *   <li>JSON integer → {@code LONG}</li>
 *   <li>JSON string → {@code STRING}</li>
 * </ul>
 * <p>
 * All other types, and any cell with an individual salt, use the object form:
 * </p>
 * <pre>{@code
 * { "salt": "<base64 or hex>", "type": "<name>", "value": <json-value> }
 * }</pre>
 * <p>
 * Redacted cells are always:
 * </p>
 * <pre>{@code
 * { "type": "X", "hash": "<base64 or hex>" }
 * }</pre>
 *
 * <h2>Type names</h2>
 * <table border="1">
 *   <caption>DataType → JSON type string</caption>
 *   <tr><th>JSON {@code "type"}</th><th>DataType</th></tr>
 *   <tr><td>{@code "STR"}</td><td>{@link DataType#STRING}</td></tr>
 *   <tr><td>{@code "LONG"}</td><td>{@link DataType#LONG}</td></tr>
 *   <tr><td>{@code "DATE"}</td><td>{@link DataType#DATE} (UTC millis)</td></tr>
 *   <tr><td>{@code "BIGINT"}</td><td>{@link DataType#BIG_INT}</td></tr>
 *   <tr><td>{@code "DEC"}</td><td>{@link DataType#BIG_DEC}</td></tr>
 *   <tr><td>{@code "BOOL"}</td><td>{@link DataType#BOOL}</td></tr>
 *   <tr><td>{@code "BYTES"}</td><td>{@link DataType#BYTES}</td></tr>
 *   <tr><td>{@code "HASH"}</td><td>{@link DataType#HASH}</td></tr>
 *   <tr><td>{@code "X"}</td><td>redacted marker (not a DataType)</td></tr>
 * </table>
 * <p>
 * {@code BIG_DEC} and {@code BIG_INT} values are always written as JSON strings
 * to preserve precision (e.g. {@code "99.9500"}). {@code BYTES} values are
 * written as lowercase hex. {@code HASH} and salt values are written as
 * 43-character Base64_32 strings; both Base64_32 and 64-character hex are
 * accepted on read.
 * </p>
 */
public class SourceRowParser implements JsonEntityParser<SourceRow> {

  /** Singleton instance. */
  public static final SourceRowParser INSTANCE = new SourceRowParser();

  // ---- JSON key constants ------------------------------------------------

  public static final String ROW_KEY   = "row";
  public static final String SALT_KEY  = "salt";
  public static final String CELLS_KEY = "cells";
  public static final String TYPE_KEY  = "type";
  public static final String VALUE_KEY = "value";
  public static final String HASH_KEY  = "hash";

  // ---- Type name constants -----------------------------------------------

  public static final String T_STR    = "STR";
  public static final String T_LONG   = "LONG";
  public static final String T_DATE   = "DATE";
  public static final String T_BIGINT = "BIGINT";
  public static final String T_DEC    = "DEC";
  public static final String T_BOOL   = "BOOL";
  public static final String T_BYTES  = "BYTES";
  public static final String T_HASH   = "HASH";
  public static final String T_REDACT = "X";


  // ========================================================================
  //  Write path: SourceRow → JSONObject
  // ========================================================================

  @Override
  public JSONObject injectEntity(SourceRow row, JSONObject jObj) {
    jObj.put(ROW_KEY, row.no());
    Optional<ByteBuffer> rowSaltOpt = row.rowSalt();
    boolean hasRowSalt = rowSaltOpt.isPresent();
    if (hasRowSalt)
      jObj.put(SALT_KEY, encodeFixed(rowSaltOpt.get()));
    JSONArray cells = new JSONArray();
    for (Cell cell : row.cells())
      cells.add(encodeCell(cell, hasRowSalt));
    jObj.put(CELLS_KEY, cells);
    return jObj;
  }


  private Object encodeCell(Cell cell, boolean hasRowSalt) {

    if (cell.isRedacted()) {
      var obj = new JSONObject();
      obj.put(TYPE_KEY, T_REDACT);
      obj.put(HASH_KEY, encodeFixed(cell.hash()));
      return obj;
    }

    DataType type = cell.dataType();
    // hasCellSalt: cell has an individual salt not covered by the row-level salt
    boolean writeCellSalt = !hasRowSalt && cell.hasSalt();

    // Compact bare form — only when no individual cell salt needed
    if (!writeCellSalt) {
      if (type == DataType.NULL)   return null;
      if (type == DataType.BOOL)   return cell.value();
      if (type == DataType.LONG)   return cell.value();   // Long
      if (type == DataType.STRING) return cell.value();   // String
    }

    // Object form
    var obj = new JSONObject();
    if (writeCellSalt)
      obj.put(SALT_KEY, encodeFixed(cell.salt()));

    if (type == DataType.NULL) {
      // Only reachable when hasCellSalt==true; value is always null
      obj.put(VALUE_KEY, null);
      return obj;
    }

    // Add "type" key for non-inferrable types (also when hasCellSalt to make
    // parsing unambiguous for DATE, which looks like LONG in JSON)
    if (!isInferrable(type))
      obj.put(TYPE_KEY, typeName(type));

    obj.put(VALUE_KEY, encodeValue(type, cell.value()));
    return obj;
  }


  private static boolean isInferrable(DataType type) {
    return switch (type) {
      case NULL, BOOL, LONG, STRING -> true;
      default -> false;
    };
  }


  private static String typeName(DataType type) {
    return switch (type) {
      case STRING  -> T_STR;
      case LONG    -> T_LONG;
      case DATE    -> T_DATE;
      case BIG_INT -> T_BIGINT;
      case BIG_DEC -> T_DEC;
      case BOOL    -> T_BOOL;
      case BYTES   -> T_BYTES;
      case HASH    -> T_HASH;
      default -> throw new IllegalArgumentException("no type name for: " + type);
    };
  }


  private static Object encodeValue(DataType type, Object value) {
    return switch (type) {
      case STRING  -> value;                                       // String
      case LONG    -> value;                                       // Long
      case DATE    -> value;                                       // Long (UTC millis)
      case BIG_INT -> value.toString();                            // "12345..."
      case BIG_DEC -> ((BigDecimal) value).toPlainString();        // "99.9500"
      case BOOL    -> value;                                       // Boolean
      case BYTES   -> IntegralStrings.toHex((ByteBuffer) value);   // hex string
      case HASH    -> encodeFixed((ByteBuffer) value);             // base64_32
      default      -> throw new IllegalArgumentException("unexpected type: " + type);
    };
  }


  // ========================================================================
  //  Read path: JSONObject → SourceRow
  // ========================================================================

  @Override
  public SourceRow toEntity(JSONObject jObj) throws JsonParsingException {

    long rowNo = JsonUtils.getNumber(jObj, ROW_KEY, true).longValue();

    String saltStr = JsonUtils.getString(jObj, SALT_KEY, false);
    ByteBuffer rowSalt = saltStr != null ? decodeFixed(saltStr, SALT_KEY) : null;

    JSONArray cellsJson = JsonUtils.getJsonArray(jObj, CELLS_KEY, true);
    int cellCount = cellsJson.size();
    Cell[] cells = new Cell[cellCount];

    // Reusable digest for deriving cell salts from the row salt
    MessageDigest workDigest =
        rowSalt != null ? SharedConstants.DIGEST.newDigest() : null;

    for (int i = 0; i < cellCount; i++)
      cells[i] = parseCell(cellsJson.get(i), rowSalt, i, workDigest);

    ByteBuffer finalRowSalt = rowSalt;
    return new SourceRow() {
      final long no = rowNo;
      final List<Cell> cellList = List.of(cells);

      @Override public long no()           { return no; }
      @Override public List<Cell> cells()  { return cellList; }

      @Override
      public Optional<ByteBuffer> rowSalt() {
        if (finalRowSalt == null || hasRedactions())
          return Optional.empty();
        return Optional.of(finalRowSalt.asReadOnlyBuffer());
      }
    };
  }


  private Cell parseCell(
      Object cellJson, ByteBuffer rowSalt, int index, MessageDigest workDigest) {

    if (cellJson == null)
      return buildNullCell(null, rowSalt, index, workDigest);

    if (cellJson instanceof Boolean b) {
      ByteBuffer data = DataType.BOOL.toByteBuffer(b);
      return buildRevealedCell(null, rowSalt, index, DataType.BOOL, data);
    }

    if (cellJson instanceof Long l) {
      ByteBuffer data = DataType.LONG.toByteBuffer(l);
      return buildRevealedCell(null, rowSalt, index, DataType.LONG, data);
    }

    if (cellJson instanceof String s) {
      ByteBuffer data = DataType.STRING.toByteBuffer(s);
      return buildRevealedCell(null, rowSalt, index, DataType.STRING, data);
    }

    if (cellJson instanceof JSONObject obj)
      return parseCellObject(obj, rowSalt, index, workDigest);

    // Bare Double: jsonimple parses decimal literals as Double — not supported
    if (cellJson instanceof Number)
      throw new JsonParsingException(
          "bare decimal numbers are not supported as cell values; " +
          "use {\"" + TYPE_KEY + "\":\"" + T_DEC + "\",\"" +
          VALUE_KEY + "\":\"99.95\"} instead");

    throw new JsonParsingException(
        "unexpected cell value type: " + cellJson.getClass().getSimpleName());
  }


  private Cell parseCellObject(
      JSONObject obj, ByteBuffer rowSalt, int index, MessageDigest workDigest) {

    String typeStr = JsonUtils.getString(obj, TYPE_KEY, false);

    // Redacted cell
    if (T_REDACT.equals(typeStr)) {
      String hashStr = JsonUtils.getString(obj, HASH_KEY, true);
      return new Cell.Redacted(decodeFixed(hashStr, HASH_KEY));
    }

    // Optional individual cell salt
    String cellSaltStr = JsonUtils.getString(obj, SALT_KEY, false);
    ByteBuffer cellSalt = cellSaltStr != null ? decodeFixed(cellSaltStr, SALT_KEY) : null;

    // Raw value from JSON (may be absent / null)
    Object rawValue = obj.get(VALUE_KEY);

    // Determine type: explicit > inferred from raw value
    DataType type = typeStr != null ? parseTypeName(typeStr) : inferType(rawValue);

    // Null value
    if (type == DataType.NULL || rawValue == null)
      return buildNullCell(cellSalt, rowSalt, index, workDigest);

    // Typed value
    Object typedValue = parseValue(type, rawValue);
    ByteBuffer data;
    try {
      data = type.toByteBuffer(typedValue);
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
    return buildRevealedCell(cellSalt, rowSalt, index, type, data);
  }


  // ---- Cell construction helpers -----------------------------------------

  /**
   * Builds a null cell, choosing the appropriate implementation in order:
   * explicit cell salt → row salt (derive cell salt) → unsalted.
   */
  private static Cell buildNullCell(
      ByteBuffer cellSalt, ByteBuffer rowSalt, int index, MessageDigest workDigest) {

    if (cellSalt != null)
      // SaltedNull.load() reads 32 bytes; pass a duplicate so original is not consumed
      return Cell.SaltedNull.load(cellSalt.duplicate());

    if (rowSalt != null) {
      
      return new Cell.RowSaltedNull(rowSalt.duplicate(), index);
    }

    return Cell.UNSALTED_NULL;
  }


  /**
   * Builds a revealed (non-null) cell, choosing the appropriate implementation
   * in order: explicit cell salt → row salt → unsalted.
   */
  private static Cell buildRevealedCell(
      ByteBuffer cellSalt, ByteBuffer rowSalt, int index, DataType type, ByteBuffer data) {

    if (cellSalt != null) {
      // SaltedCell expects a single ByteBuffer: [32-byte salt | data bytes]
      ByteBuffer combined =
          ByteBuffer.allocate(SharedConstants.HASH_WIDTH + data.remaining());
      combined.put(cellSalt.duplicate()).put(data.duplicate()).flip();
      return new Cell.SaltedCell(combined, type);
    }

    if (rowSalt != null)
      // RowSaltedCell slices rowSalt internally; safe to reuse across cells
      return new Cell.RowSaltedCell(rowSalt, index, type, data);

    return new Cell.UnsaltedReveal(type, data);
  }


  // ---- Type parsing helpers ----------------------------------------------

  private static DataType parseTypeName(String typeStr) {
    return switch (typeStr) {
      case T_STR    -> DataType.STRING;
      case T_LONG   -> DataType.LONG;
      case T_DATE   -> DataType.DATE;
      case T_BIGINT -> DataType.BIG_INT;
      case T_DEC    -> DataType.BIG_DEC;
      case T_BOOL   -> DataType.BOOL;
      case T_BYTES  -> DataType.BYTES;
      case T_HASH   -> DataType.HASH;
      default -> throw new JsonParsingException("unknown type name: \"" + typeStr + "\"");
    };
  }


  private static DataType inferType(Object value) {
    if (value == null)              return DataType.NULL;
    if (value instanceof Boolean)   return DataType.BOOL;
    if (value instanceof Long)      return DataType.LONG;
    if (value instanceof String)    return DataType.STRING;
    throw new JsonParsingException(
        "cannot infer DataType from " + value.getClass().getSimpleName() +
        "; add a \"" + TYPE_KEY + "\" field");
  }


  private static Object parseValue(DataType type, Object rawValue) {
    try {
      return switch (type) {
        case STRING  -> (String) rawValue;
        case LONG    -> ((Number) rawValue).longValue();
        case DATE    -> ((Number) rawValue).longValue();   // Long → accepted by toDateBuffer
        case BIG_INT -> new BigInteger((String) rawValue);
        case BIG_DEC -> new BigDecimal((String) rawValue);
        case BOOL    -> (Boolean) rawValue;
        case BYTES   -> ByteBuffer.wrap(
                            IntegralStrings.hexToBytes((String) rawValue));
        case HASH    -> decodeFixed((String) rawValue, VALUE_KEY);
        default      -> throw new JsonParsingException("unsupported type: " + type);
      };
    } catch (JsonParsingException jpx) {
      throw jpx;
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
  }


  // ---- Byte encoding / decoding ------------------------------------------

  /**
   * Encodes a 32-byte buffer as a 43-character Base64_32 string.
   * The buffer's position is not modified.
   */
  private static String encodeFixed(ByteBuffer buf) {
    return Base64_32.encode(buf);
  }


  /**
   * Decodes a 43-character Base64_32 string or a 64-character hex string
   * to a 32-byte {@code ByteBuffer}.
   *
   * @param s           encoded string
   * @param fieldName   JSON field name (used in error messages)
   */
  private static ByteBuffer decodeFixed(String s, String fieldName) {
    final int hexLen = SharedConstants.HASH_WIDTH * 2;
    byte[] bytes;
    try {
      if (s.length() == Base64_32.ENC_LEN)
        bytes = Base64_32.decode(s);
      else if (s.length() == hexLen)
        bytes = IntegralStrings.hexToBytes(s);
      else
        throw new JsonParsingException(
            "invalid \"%s\" value (expected %d-char base64 or %d-char hex): %s"
            .formatted(fieldName, Base64_32.ENC_LEN, hexLen, s));
    } catch (JsonParsingException jpx) {
      throw jpx;
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
    assert bytes.length == SharedConstants.HASH_WIDTH;
    
    return ByteBuffer.wrap(bytes);
  }

}
