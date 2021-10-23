/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import static io.crums.sldg.sql.Config.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Properties;

import io.crums.io.FileUtils;
import io.crums.sldg.SldgConstants;
import io.crums.util.IntegralStrings;
import io.crums.util.TidyProperties;

/**
 * {@linkplain Config} file builder.
 * 
 * @see #toConfig()
 * @see #toProperties(boolean)
 * @see #save()
 */
public class ConfigFileBuilder {
  
  private final static String PREAMBLE =
      " Namespace Summary:\n" +
      " =================\n\n " +
      SOURCE_JDBC_URL + ":\n " +
      " Connection URL (jdbc:) to source table (required; read-only OK)\n\n " +
      SOURCE_INFO_PREFIX + "xyz:\n " +
      " Connection property xyz is prefixed with " + SOURCE_INFO_PREFIX + "\n\n " +
      SOURCE_JDBC_DRIVER + ":\n " +
      " JDBC driver class name needed to load above jdbc URL (optional)\n\n " +
      SOURCE_JDBC_DRIVER_CLASSPATH + ":\n " +
      " Path to .jar bundle containing the named driver class (optional)\n\n " +
      SOURCE_QUERY_SIZE + ":\n " +
      " \"SELECT COUNT(*)\" from source table query (required)\n\n " +
      SOURCE_QUERY_ROW + ":\n " +
      " \"SELECT\" by row-number prepared-statement query (required)\n " +
      " Note however you design this, your row-nums must range [1, size] (no gaps)\n\n " +
      SOURCE_SALT_SEED + ":\n " +
      " Secret 64-char hex value seed for generating table-cell salts by row/col coordinates\n " +
      " Note this value should not be changed or lost. It protects individual table-cell\n " +
      " values from rainbow attacks (reverse-engineering a value from its hash).\n\n " +
      HASH_JDBC_URL + ":\n " +
      " Connection URL to DB the tracking hash-ledger lives in (optional)\n " +
      " If not set, then the hash-ledger lives in the same DB as the source-table\n\n " +
      HASH_INFO_PREFIX + "xyz:\n " +
      " Connection property xyz is prefixed with " + HASH_INFO_PREFIX + "\n\n " +
      HASH_JDBC_DRIVER + ":\n " +
      " JDBC driver class name needed to load above jdbc URL (optional)\n\n " +
      HASH_JDBC_DRIVER_CLASSPATH + ":\n " +
      " Path to .jar bundle containing the named driver class (optional)\n\n " +
      HASH_TABLE_PREFIX + ":\n " +
      " Hash-ledger tables (3) use this prefix in their table names. Usually set to\n " +
      " the source table's name.\n\n " +

      "The following 3 specify the SQL schemas for the hash tables. They may be\n " +
      "DB vendor specific.\n\n " +
      
      HASH_SCHEMA_SKIP + ":\n " +
      " SQL schema (CREATE TABLE statement) for the skipledger table (defaulted)\n\n " +
      HASH_SCHEMA_CHAIN + ":\n " +
      " SQL schema (CREATE TABLE statement) for the chain table (defaulted)\n\n " +
      HASH_SCHEMA_TRAIL + ":\n " +
      " SQL schema (CREATE TABLE statement) for the trail table (defaulted)\n\n " +
      
      "Note, driver classpaths above may either be set absolutely, or relative to\n " +
      "the location of this file.\n\n";
  
  private final Properties aux = new Properties();
  
  private final File configFile;
  private final File baseDir;
  

  private String srcUrl;



  private String srcDriverClass;
  private String srcDriverCp;
  

  
  private String hashUrl;
  private String hashDriverClass;
  private String hashDriverCp;
  
  private String hashTablePrefix;
  private String hashSkipSchema;
  private String hashChainSchema;
  private String hashTrailSchema;
  
  /**
   * The source seed salt in hex.
   */
  private String srcSalt;
  private String srcSizeQuery;
  private String srcRowQuery;
  
  

  /**
   * 
   */
  public ConfigFileBuilder(File configFile) {
    this.configFile = Objects.requireNonNull(configFile, "null configFile").getAbsoluteFile();
    if (this.configFile.exists())
      throw new IllegalArgumentException("config file path already exists: " + this.configFile);
    this.baseDir = FileUtils.getParentDir(configFile);
  }
  
  
  
  public void setSourceUrl(String srcUrl) {
    this.srcUrl = srcUrl;
  }



  public String getSourceUrl() {
    return srcUrl;
  }
  
  
  public void setSourceConProperty(String name, String value) {
    setConProperty(name, value, SOURCE_INFO_PREFIX);
  }
  
  
  public Properties getSourceConProperties() {
    return TidyProperties.subProperties(aux, SOURCE_INFO_PREFIX);
  }
  
  
  
  public boolean isValid() {
    try {
      toConfig();
      return true;
    } catch (IllegalStateException isx) {
      return false;
    }
  }
  
  
  
  
  
  public Config toConfig() throws IllegalStateException {
    Properties props = toProperties(true);
    try {
      return new Config(props);
    } catch (IllegalArgumentException iax) {
      throw new IllegalStateException(iax.getMessage());
    }
  }
  
  
  
  public Properties toProperties(boolean includeBase) {
    Properties props = new TidyProperties(PROP_NAMES);
    props.putAll(aux);
    set(props, SOURCE_JDBC_URL, srcUrl);
    set(props, SOURCE_JDBC_DRIVER, srcDriverClass);
    set(props, SOURCE_JDBC_DRIVER_CLASSPATH, srcDriverCp);
    set(props, HASH_JDBC_URL, hashUrl);
    set(props, HASH_JDBC_DRIVER, hashDriverClass);
    set(props, SOURCE_JDBC_DRIVER, srcDriverClass);
    set(props, HASH_JDBC_DRIVER_CLASSPATH, hashDriverCp);
    set(props, HASH_TABLE_PREFIX, hashTablePrefix);
    set(props, HASH_SCHEMA_SKIP, hashSkipSchema);
    set(props, HASH_SCHEMA_TRAIL, hashTrailSchema);
    set(props, HASH_SCHEMA_CHAIN, hashChainSchema);
    set(props, SOURCE_SALT_SEED, srcSalt);
    set(props, SOURCE_QUERY_SIZE, srcSizeQuery);
    set(props, SOURCE_QUERY_ROW, srcRowQuery);
    if (includeBase)
      props.put(BASE_DIR, baseDir.getAbsolutePath());
    return props;
  }
  
  
  
  public void save() {
    Properties props = toProperties(false);
    {
      File parentDir = configFile.getParentFile();
      if (parentDir != null)
        FileUtils.ensureDir(parentDir);
    }
    try (var out = new FileOutputStream(configFile)) {
      props.store(out, PREAMBLE);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "while attempting to write to <" + configFile + ">: " + iox, iox);
    }
  }
  
  

  private void set(Properties props, String name, String value) {
    if (value != null)
      props.put(name, value);
  }



  /**
   * Returns the configuration file. May not yet exist.
   */
  public final File getConfigFile() {
    return configFile;
  }



  /**
   * Returns the base directory.
   */
  public final File getBaseDir() {
    return baseDir;
  }



  /**
   * @return the srcDriverClass
   */
  public String getSourceDriverClass() {
    return srcDriverClass;
  }



  /**
   * @param srcDriverClass the srcDriverClass to set
   */
  public void setSourceDriverClass(String srcDriverClass) {
    this.srcDriverClass = srcDriverClass;
  }



  /**
   * @return the srcDriverCp
   */
  public String getSourceDriverClasspath() {
    return srcDriverCp;
  }



  /**
   * @param srcDriverCp the srcDriverCp to set
   */
  public void setSourceDriverClasspath(String srcDriverCp) {
    this.srcDriverCp = srcDriverCp;
  }



  /**
   * @return the hashUrl
   */
  public String getHashUrl() {
    return hashUrl;
  }



  /**
   * @param hashUrl the hashUrl to set
   */
  public void setHashUrl(String hashUrl) {
    this.hashUrl = hashUrl;
  }
  
  
  public void setHashConProperty(String name, String value) {
    setConProperty(name, value, HASH_INFO_PREFIX);
  }
  
  
  public Properties getHashConProperties() {
    return TidyProperties.subProperties(aux, HASH_INFO_PREFIX);
  }
  
  
  
  private void setConProperty(String name, String value, String prefix) {
    name = name.trim();
    if (name.isEmpty())
      throw new IllegalArgumentException("empty property name");
    name = prefix + name;
    if (value == null)
      aux.remove(name);
    else
      aux.put(name, value);
  }



  /**
   * @return the hashDriverClass
   */
  public String getHashDriverClass() {
    return hashDriverClass;
  }



  /**
   * @param hashDriverClass the hashDriverClass to set
   */
  public void setHashDriverClass(String hashDriverClass) {
    this.hashDriverClass = hashDriverClass;
  }



  /**
   * @return the hashDriverCp
   */
  public String getHashDriverClasspath() {
    return hashDriverCp;
  }



  /**
   * @param hashDriverCp the hashDriverCp to set
   */
  public void setHashDriverClasspath(String hashDriverCp) {
    this.hashDriverCp = hashDriverCp;
  }



  /**
   * @return the hashTablePrefix
   */
  public String getHashTablePrefix() {
    return hashTablePrefix;
  }



  /**
   * @param hashTablePrefix the hashTablePrefix to set
   */
  public void setHashTablePrefix(String hashTablePrefix) {
    this.hashTablePrefix = hashTablePrefix;
  }



  /**
   * @return the srcSalt
   */
  public final String getSrcSalt() {
    return srcSalt;
  }



  public void seedSourceSalt() {
    byte[] seed = new byte[SldgConstants.HASH_WIDTH];
    new SecureRandom().nextBytes(seed);
    srcSalt = IntegralStrings.toHex(seed);
  }



  public String getSourceSizeQuery() {
    return srcSizeQuery;
  }



  public void setSourceSizeQuery(String srcSizeQuery) {
    this.srcSizeQuery = srcSizeQuery;
  }



  public String getSourceRowQuery() {
    return srcRowQuery;
  }



  public void setSourceRowQuery(String srcRowQuery) {
    this.srcRowQuery = srcRowQuery;
  }



  public String getHashSkipSchema() {
    return hashSkipSchema;
  }



  public void setHashSkipSchema(String hashSkipSchema) {
    this.hashSkipSchema = hashSkipSchema;
  }



  public String getHashChainSchema() {
    return hashChainSchema;
  }



  public void setHashChainSchema(String hashChainSchema) {
    this.hashChainSchema = hashChainSchema;
  }
  
  
  
  public void setDefaultSkipSchema() {
    checkTablePrefix();
    this.hashSkipSchema =
        HashLedgerSchema.protoSkipTableSchema(hashTablePrefix);
  }
  
  
  private void checkTablePrefix() {
    if (hashTablePrefix == null || hashTablePrefix.isBlank())
      throw new IllegalStateException("hash table prefix not set");
  }
  
  
  public void setDefaultChainSchema() {
    checkTablePrefix();
    this.hashChainSchema = HashLedgerSchema.protoChainTableSchema(hashTablePrefix);
  }



  public String getHashTrailSchema() {
    return hashTrailSchema;
  }



  public void setHashTrailSchema(String hashTrailSchema) {
    this.hashTrailSchema = hashTrailSchema;
  }
  
  
  public void setDefaultTrailSchema() {
    checkTablePrefix();
    this.hashTrailSchema = HashLedgerSchema.protoTrailTableSchema(hashTablePrefix);
  }
  
  
  
  public void setDefaultHashSchemas() {
    setDefaultSkipSchema();
    setDefaultTrailSchema();
    setDefaultChainSchema();
  }
  
  public void setDefaultHashSchemas(String hashTablePrefix) {
    setHashTablePrefix(hashTablePrefix);
    setDefaultHashSchemas();
  }

}
