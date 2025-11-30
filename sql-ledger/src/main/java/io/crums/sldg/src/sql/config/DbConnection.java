/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public record DbConnection(
    String url, Optional<String> driverClass, Optional<DbCredentials> creds) {
  
  /**
   * 
   * @param url         jdbc URL. For eg, {@code jdbc:mysql://localhost:3306/mydb}
   * @param driverClass fully qualified driver class name
   * @param creds       connection credentials (username / password)
   */
  public DbConnection {
    
    try {
      URI uri = new URI(url);
      if (!"jdbc".equals(uri.getScheme()))
        throw new IllegalArgumentException(
            "connection URL must use 'jdbc' scheme; " + url);
    } catch (URISyntaxException usx) {
      var detail = usx.getMessage();
      throw new IllegalArgumentException(
          "malformed url: %s%nDetail: %s"
          .formatted(url, detail == null ? "" : detail),
          usx);
    }
    if (driverClass.filter(String::isEmpty).isPresent())
      driverClass = Optional.empty();
    
    if (creds == null)
      creds = Optional.empty();
  }
  
  
  /**
   * 
   * @param url         jdbc URL. For eg, {@code jdbc:mysql://localhost:3306/mydb}
   * @param driverClass fully qualified driver class name, or {@code null}
   * @param creds       may be {@code null}
   */
  public DbConnection(
      String url, String driverClass, DbCredentials creds) {
    this(
        url,
        Optional.ofNullable(driverClass),
        Optional.ofNullable(creds));
  }
  
  
  /**
   * 
   * @param url         jdbc URL. For eg, {@code jdbc:mysql://localhost:3306/mydb}
   * @param driverClass fully qualified driver class name, or {@code null}
   * @param username    not-empty
   * @param password    not-empty
   */
  public DbConnection(
      String url, String driverClass, String username, String password) {
    this(
        url,
        Optional.ofNullable(driverClass),
        Optional.of(new DbCredentials(username, password)));
  }
  
  
  
  
  public static class Parser implements JsonEntityParser<DbConnection> {
    
    public final static String URL = "url";
    public final static String DRIVER_CLASS = "driver_class";

    @Override
    public JSONObject injectEntity(DbConnection dbCon, JSONObject jObj) {
      jObj.put(URL, dbCon.url());
      dbCon.driverClass().ifPresent(dc -> jObj.put(DRIVER_CLASS, dc));
      dbCon.creds().ifPresent(c -> DbCredentials.PARSER.injectEntity(c, jObj));
      return jObj;
    }

    @Override
    public DbConnection toEntity(JSONObject jObj) throws JsonParsingException {
      String url = JsonUtils.getString(jObj, URL, true);
      String driverClass = JsonUtils.getString(jObj, DRIVER_CLASS, false);
      Optional<DbCredentials> creds =
          JsonUtils.getString(jObj, DbCredentials.Parser.USERNAME, false) != null ?
              Optional.of(DbCredentials.PARSER.toEntity(jObj)) :
                Optional.empty();
      try {
        return new DbConnection(url, Optional.ofNullable(driverClass), creds);
      
      } catch (IllegalArgumentException iax) {
        throw new JsonParsingException(iax);
      }
    }
    
  }
  
  
  

}
