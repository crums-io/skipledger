/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql.config;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * Database credentials.
 * 
 * @see DbCredentials#PARSER
 */
public record DbCredentials(String username, String password) {
  

  /** JSON parser. */
  public final static JsonEntityParser<DbCredentials> PARSER = new Parser();
  
  /**
   * Full constructor.
   * 
   * @param username    not empty
   * @param password    not empty
   */
  public DbCredentials {
    if (username.isEmpty())
      throw new IllegalArgumentException("empty username");
    if (password.isEmpty())
      throw new IllegalArgumentException("empty password");
  }
  
  
  public static class Parser implements JsonEntityParser<DbCredentials> {

    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";
    

    @Override
    public JSONObject injectEntity(DbCredentials creds, JSONObject jObj) {
      jObj.put(USERNAME, creds.username());
      jObj.put(PASSWORD, creds.password());
      return jObj;
    }

    @Override
    public DbCredentials toEntity(JSONObject jObj) throws JsonParsingException {
      var username = JsonUtils.getString(jObj, USERNAME, true);
      var password = JsonUtils.getString(jObj, PASSWORD, true);
      try {
        return new DbCredentials(username, password);
      } catch (Exception x) {
        throw new JsonParsingException(x);
      }
    }
    
  }

}
