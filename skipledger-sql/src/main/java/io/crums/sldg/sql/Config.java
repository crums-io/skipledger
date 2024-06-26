/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

import io.crums.io.FileUtils;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.depra.json.SourceInfoParser;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.src.TableSalt;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.TidyProperties;

/**
 * SQL data source configuration. Instances are immutable.
 * 
 * <h2>Quirks and Features</h2>
 * <p>
 * A simple <em>properties file</em> is used to store configuration. (This could have been
 * JSON, but honestly, this seems simpler.)
 * </p>
 * <h3>Relative Paths</h3>
 * <p>
 * Filepaths may be specified in either absolute or relative form. For relative
 * paths, <em>paths are resolved relative to the location of the configuration
 * file.</em>
 * </p>
 * <h3>Credentials</h3>
 * <p>
 * DB access credentials may be stored in the file this loads from. This is questionable
 * practice. Will address in a future release.
 * </p>
 */
public class Config {
  
  /**
   * Every property known to this configuration is prefixed with this value.
   */
  public final static String ROOT = "sldg.";
  
  /**
   * The name of the base directory path. <em>This value should not be set in the properties file.</em>
   * It is set dynamically to the parent directory of the configuration file.
   */
  public final static String BASE_DIR = ROOT + "base.dir";
  /**
   * The name of the JDBC connection URL to the database the source ledger lives in. Required property.
   */
  public final static String SOURCE_JDBC_URL = ROOT + "source.jdbc.url";
  /**
   * The name of the fully-qualified classname of the JDBC driver. If not provided, then it's
   * assumed a suitable {@linkplain Driver} is already registered for the given {@linkplain #SOURCE_JDBC_URL} value.
   */
  public final static String SOURCE_JDBC_DRIVER = ROOT +  "source.jdbc.driver";
  /**
   * The name of the classpath value for the {@linkplain #SOURCE_JDBC_DRIVER} class. If provided,
   * then the {@linkplain Driver} class is loaded from the bundle (.jar) specified by this endpoint.
   */
  public final static String SOURCE_JDBC_DRIVER_CLASSPATH = ROOT +  "source.jdbc.driver.classpath";
  
  
  /**
   * The name of the 64-character hexadecimal value used as seed to salt individual
   * table cell values.
   */
  public final static String SOURCE_SALT_SEED = ROOT + "source.salt.seed";
  /**
   * The name of the value string specifying the "SELECT COUNT(*)" query.
   * 
   * @see #SOURCE_QUERY_ROW
   */
  public final static String SOURCE_QUERY_SIZE =  ROOT + "source.query.size";
  /**
   * The name of the value string specifying the "SELECT" by row-number query. Note this is
   * used to construct a {@linkplain PreparedStatement} (so it requires a '?' parameter as
   * the row number. Note, however you design this query, the row-numbering must be contiguous
   * (no gaps), ranging from 1 to "size()".
   * 
   * @see #SOURCE_QUERY_SIZE
   */
  public final static String SOURCE_QUERY_ROW =  ROOT + "source.query.row";
  
  
  /**
   * The name of the JDBC connection URL to the database the hash ledger lives in. If not
   * provided, then this means source and hash ledgers live in the same database.
   */
  public final static String HASH_JDBC_URL =  ROOT + "hash.jdbc.url";
  public final static String HASH_JDBC_DRIVER =  ROOT + "hash.jdbc.driver";
  public final static String HASH_JDBC_DRIVER_CLASSPATH =  ROOT + "hash.jdbc.driver.classpath";
  
  /**
   * The name of the prefix for the backing hash tables. In most cases, this will be just
   * named after the source table/view.
   */
  public final static String HASH_TABLE_PREFIX = ROOT + "hash.table.prefix";
  
  
  
  
  public final static String HASH_SCHEMA_SKIP = ROOT + "hash.schema.skip";

  public final static String HASH_SCHEMA_CHAIN = ROOT + "hash.schema.chain";

  public final static String HASH_SCHEMA_TRAIL = ROOT + "hash.schema.trail";
  
  
  public final static String SOURCE_INFO_PREFIX = ROOT + "source.info.";
  public final static String HASH_INFO_PREFIX = ROOT + "hash.info.";
  
  
  /**
   * Path to JSON meta file.
   */
  public final static String META_PATH = ROOT + "meta.path";
  
  /**
   * List of ordered property names. Controls the order they're presented in the properties
   * file.
   * 
   * @see #getTidyProperties()
   */
  public final static List<String> PROP_NAMES = Lists.asReadOnlyList(
    new String[] {
        BASE_DIR,
        META_PATH,
        
        SOURCE_JDBC_URL,
        SOURCE_JDBC_DRIVER,
        SOURCE_JDBC_DRIVER_CLASSPATH,
        SOURCE_QUERY_SIZE,
        SOURCE_QUERY_ROW,
        SOURCE_SALT_SEED,
        
        HASH_JDBC_URL,
        HASH_JDBC_DRIVER,
        HASH_JDBC_DRIVER_CLASSPATH,
        HASH_TABLE_PREFIX,
        HASH_SCHEMA_SKIP,
        HASH_SCHEMA_CHAIN,
        HASH_SCHEMA_TRAIL,
    });
  
  
  
  /**
   * Loads the given properties file and injects the {@linkplain #BASE_DIR} property
   * using the file's location (used to resolve relative paths).
   */
  public static Properties loadProperties(File propertiesFile) {
    Properties props = new Properties();
    try (var in = new FileInputStream(propertiesFile)) {
      props.load(in);
    } catch (FileNotFoundException fnfx) {
      throw new IllegalArgumentException("properties file does not exist: " + propertiesFile);
    } catch (IOException iox) {
      throw new IllegalArgumentException("failed to read properties file: " + propertiesFile, iox);
    }
    File baseDir = FileUtils.getParentDir(propertiesFile);
    props.put(BASE_DIR, baseDir.getAbsolutePath());
    return props;
  }
  
  
  protected final File baseDir;
  
  private final String srcUrl;
  private final String srcDriverClass;
  private final String srcDriverCp;
  
  
  private ConnectionInfo srcConInfo() {
    return new ConnectionInfo(srcUrl, srcDriverClass, srcDriverCp);
  }
  
  private final String hashUrl;
  private final String hashDriverClass;
  private final String hashDriverCp;
  
  
  private ConnectionInfo hashConInfo() {
    ConnectionInfo con = srcConInfo();
    if (dedicatedSourceCon())
      con = new ConnectionInfo(hashUrl, hashDriverClass, hashDriverCp, con);
    return con;
  }
  
  
  private final String hashTablePrefix;
//  private final String hashAutoInc;
  private final String hashSkipSchema;
  private final String hashChainSchema;
  private final String hashTrailSchema;
  
  /**
   * The source seed salt in hex.
   */
  private final String srcSalt;
  private final String srcSizeQuery;
  private final String srcRowQuery;
  
  private final Properties aux;
  
  private final String metaPath;
  
  
  
  public Config(File propertiesFile) {
    this(loadProperties(propertiesFile));
  }
  
  public Config(Properties props) {
    this.baseDir = getBaseDir(props);
    
    this.srcUrl = props.getProperty(SOURCE_JDBC_URL);
    enforceRequired(SOURCE_JDBC_URL, srcUrl);
    
    this.srcDriverClass = props.getProperty(SOURCE_JDBC_DRIVER);
    this.srcDriverCp = props.getProperty(SOURCE_JDBC_DRIVER_CLASSPATH);
    enforceMeaningfulCp(srcDriverCp, srcDriverClass, SOURCE_JDBC_DRIVER_CLASSPATH, SOURCE_JDBC_DRIVER);
    
    
    this.hashUrl = props.getProperty(HASH_JDBC_URL);
    this.hashDriverClass = props.getProperty(HASH_JDBC_DRIVER);
    this.hashDriverCp = props.getProperty(HASH_JDBC_DRIVER_CLASSPATH);
    
    enforceMeaningfulCp(hashDriverCp, hashDriverClass, HASH_JDBC_DRIVER_CLASSPATH, HASH_JDBC_DRIVER);
    
    this.hashTablePrefix = props.getProperty(HASH_TABLE_PREFIX);
    
    enforceRequired(HASH_TABLE_PREFIX, hashTablePrefix);
    
    this.hashSkipSchema = props.getProperty(HASH_SCHEMA_SKIP);
    this.hashChainSchema = props.getProperty(HASH_SCHEMA_CHAIN);
    this.hashTrailSchema = props.getProperty(HASH_SCHEMA_TRAIL);
    
    this.srcSalt = getSrcSalt(props);
    
    this.srcSizeQuery = props.getProperty(SOURCE_QUERY_SIZE);
    enforceRequired(SOURCE_QUERY_SIZE, srcSizeQuery);
    
    this.srcRowQuery = props.getProperty(SOURCE_QUERY_ROW);
    enforceRequired(SOURCE_QUERY_ROW, srcRowQuery);
    
    this.aux = makeAux(props);
    
    this.metaPath = props.getProperty(META_PATH);
  }
  
  
  
  public Optional<SourceInfo> getSourceInfo() {
    if (metaPath == null)
      return Optional.empty();
    
    File file = FileUtils.getRelativeUnlessAbsolute(metaPath, baseDir);
    if (!file.isFile())
      return Optional.empty();
    
    SourceInfo srcInfo;
    try {
      srcInfo = SourceInfoParser.INSTANCE.toEntity(file);
    
    } catch (Exception x) {
      Logger.getGlobal().warning("Failed to parse " + file + " :: " + x.getMessage());
      srcInfo = null;
    }
    return Optional.ofNullable(srcInfo);
  }
  
  
  private Properties makeAux(Properties props) {
    Properties aux = new Properties(props);
    aux.keySet().removeAll(propNames());
    return aux;
  }
  
  
  
  private Properties getSrcInfo() {
    return getSubProperties(SOURCE_INFO_PREFIX);
  }
  
  
  private Properties getHashInfo() {
    return getSubProperties(HASH_INFO_PREFIX);
  }
  
  
  private Properties getSubProperties(String prefix) {
    return TidyProperties.subProperties(aux, prefix);
  }
  
  
  private File getBaseDir(Properties props) {
    String baseDir = props.getProperty(BASE_DIR);
    enforceRequired(BASE_DIR, baseDir);
    File base = new File(baseDir);
    if (!base.isDirectory())
      throw new IllegalArgumentException(BASE_DIR + ": " + baseDir + " not a directory");
    return base.isAbsolute() ? base : base.getAbsoluteFile();
  }
  
  private String getSrcSalt(Properties props) {
    String hex = props.getProperty(SOURCE_SALT_SEED);
    enforceRequired(SOURCE_SALT_SEED, hex);
    if (!IntegralStrings.isHex(hex) || hex.length() != 2 * SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException(SOURCE_SALT_SEED + ": " + hex);
    return hex;
  }
  
  
  private void enforceRequired(String name, String value) {
    if (!isSet(value))
      throw new IllegalArgumentException("required property not set: " + name);
  }
  
  
  private boolean isSet(String value) {
    return value != null && !value.isEmpty();
  }
  
  
  private void enforceMeaningfulCp(String driverCp, String driverClass, String cpPropName, String driverPropName) {
    if (isSet(driverCp)) {
      if (!driverCp.endsWith(".jar"))
        throw new IllegalArgumentException("Only .jar files are supported. " + cpPropName + ": " + driverCp);
      if (!isSet(driverClass))
        throw new IllegalArgumentException(cpPropName + " set while " + driverPropName + " is not");
    }
  }
  
  
  /**
   * Returns the state of this instance as a properties object.
   * 
   * @return {@linkplain #getTidyProperties()}
   */
  public Properties getProperties() {
    return getTidyProperties();
  }


  /**
   * Returns the state of this instance as a properties object.
   * 
   * @see #PROP_NAMES
   */
  public TidyProperties getTidyProperties() {
    TidyProperties props = new TidyProperties(propNames());
    props.putAll(aux);
    props.put(SOURCE_JDBC_URL, srcUrl);
    set(props, SOURCE_JDBC_DRIVER, srcDriverClass);
    set(props, SOURCE_JDBC_DRIVER_CLASSPATH, srcDriverCp);
    set(props, HASH_JDBC_URL, hashUrl);
    set(props, HASH_JDBC_DRIVER, hashDriverClass);
    set(props, SOURCE_JDBC_DRIVER, srcDriverClass);
    set(props, HASH_JDBC_DRIVER_CLASSPATH, hashDriverCp);
    props.put(HASH_TABLE_PREFIX, hashTablePrefix);
    props.put(HASH_SCHEMA_SKIP, hashSkipSchema);
    props.put(HASH_SCHEMA_CHAIN, hashChainSchema);
    props.put(HASH_SCHEMA_TRAIL, hashTrailSchema);
    props.put(SOURCE_SALT_SEED, srcSalt);
    props.put(SOURCE_QUERY_SIZE, srcSizeQuery);
    props.put(SOURCE_QUERY_ROW, srcRowQuery);
    return props;
  }
  
  
  /**
   * Returns the property names, ordered as they should appear in the properties file.
   * Contract: <em>does not depend on constructor arguments.</em>
   */
  protected List<String> propNames() {
    return PROP_NAMES;
  }
  
  
  private void set(Properties props, String name, String value) {
    if (value != null)
      props.put(name, value);
  }
  

  public TableSalt getSourceSalt() {
    return new TableSalt(IntegralStrings.hexToBytes(srcSalt));
  }
  
  
  public String getHashTablePrefix() {
    return hashTablePrefix;
  }
  
  
//  public String getHashAutoIncKeyword() {
//    return hashAutoInc;
//  }
  
  public HashLedgerSchema getHashLedgerSchema() {
    return new HashLedgerSchema(this);
  }
  
  
  public boolean dedicatedSourceCon() {
    return hashUrl != null && ! hashUrl.equals(srcUrl);
  }
  
  
  public Connection getConnection() {
    if (!dedicatedSourceCon())
      throw new IllegalStateException("instance uses different JDBC connections for source- and hash-ledgers");
    return getSourceConnection();
  }
  
  
  
  public Connection getSourceConnection() {
    return srcConInfo().open(baseDir, getSrcInfo());
  }
  
  
  public Connection getHashConnection() {
    if (hashUrl == null)
      return getSourceConnection();
    
    return hashConInfo().open(baseDir, getHashInfo());
  }
  
  
  
  public SqlSourceQuery.Builder getSourceBuilder() {
    return new SqlSourceQuery.DirectBuilder(srcSizeQuery, srcRowQuery);
  }
  
  
  
  public String getSkipTableSchema() {
    return hashSkipSchema;
  }
  
  
  public String getChainTableSchema() {
    return hashChainSchema;
  }
  
  
  public String getTrailTableSchema() {
    return hashTrailSchema;
  }
  
}








