/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import io.crums.io.FileUtils;

/**
 * Encapsulates {@linkplain Driver} and connection URL information. Password
 * info deliberately kept out.
 */
public class ConnectionInfo {
  
  private final String url;
  private final String driverClassname;
  private final String driverClasspath;
  
  // FIXME: use DriverManager to implement nesting. Buggy as implemented.
  private final ConnectionInfo parent;

  

  public ConnectionInfo(String url, String driverClassname, String driverClasspath) {
    this(url, driverClassname, driverClasspath, null);
  }
  
  /**
   * 
   * @param url
   * @param driverClassname
   * @param driverClasspath
   * @param parent
   */
  public ConnectionInfo(
      String url, String driverClassname, String driverClasspath, ConnectionInfo parent) {
    
    this.url = Objects.requireNonNull(url, "null connection url");
    this.driverClassname = driverClassname;
    this.driverClasspath = driverClasspath;
    this.parent = parent;
    
    if (this.driverClasspath != null && this.driverClassname == null)
      throw new IllegalArgumentException(
          "driverClass is null while driverClasspath is not: " + driverClasspath);
  }
  
  
  public Connection open(File baseDir, Properties info) throws SqlLedgerException {
    try {
      String driverClassname = resolveDriverClassname();
      if (driverClassname == null)
        return info == null ?
            DriverManager.getConnection(url) : DriverManager.getConnection(url, info);
      
      String driverClasspath = resolveDriverClasspath();
      
      Class<?> clazz;
      if (driverClasspath == null)
        try {
          clazz = Class.forName(driverClassname);
        } catch (ClassNotFoundException cnfx) {
          throw new SqlLedgerException("failed to load driver class " + driverClassname, cnfx);
        }
      else {
        File classpath = FileUtils.getRelativeUnlessAbsolute(driverClasspath, baseDir);
        if (!classpath.isFile())
          throw new SqlLedgerException(
              driverClasspath + " resolved to " + classpath + " is not an existing jar file");
        
        URL url;
        try {
          url = new URL("jar", "",  classpath.toURI().toURL() + "!/");
        
        } catch (MalformedURLException mux) {
          throw new SqlLedgerException("on constructing jar URL for " + classpath, mux);
        }
        
        URLClassLoader classLoader = new URLClassLoader(new URL[] { url });
        try {
          clazz = Class.forName(driverClassname, true, classLoader);
        
        } catch (ReflectiveOperationException | IllegalArgumentException | SecurityException x) {
          throw new SqlLedgerException("on loading " + driverClassname + " from " + classpath, x);
        }
      }
      
      Driver driver;
      try {
        driver = (Driver) clazz.getDeclaredConstructor().newInstance();
      } catch (ReflectiveOperationException | IllegalArgumentException | SecurityException x) {
        throw new SqlLedgerException("on instantiating driver class " + clazz, x);
      }

      return driver.connect(url, info);
    } catch (SQLException sx) {
      throw new SqlLedgerException("on connecting with " + url);
    }
  }
  
  
  
  
  /**
   * Resolves the driver class name. If it's not found here, the {@linkplain #getParent() parent}
   * (if any) is consulted.
   * 
   * @return possibly {@code null}
   */
  public String resolveDriverClassname() {
    return driverClassname == null && parent != null ? parent.driverClassname : driverClassname;
  }

  /**
   * Resolves the driver classpath. If it's not found here, the {@linkplain #getParent() parent}
   * (if any) is consulted.
   * 
   * @return possibly {@code null}
   */
  public String resolveDriverClasspath() {
    return driverClasspath == null && parent != null ? parent.driverClasspath : driverClasspath;
  }

  /**
   * Returns the JDBC URL (connection string).
   * 
   * @return never {@code null}
   */
  public final String getUrl() {
    return url;
  }

  /**
   * Returns the driver class name.
   * 
   * @return possibly {@code null}
   */
  public final String getDriverClassname() {
    return driverClassname;
  }

  /**
   * Returns the driver classpath.
   * 
   * @return possibly {@code null}
   */
  public final String getDriverClasspath() {
    return driverClasspath;
  }

  /**
   * Returns the parent instance to which values default to if this instance's class name
   * or classpath is {@code null}.
   */
  public final ConnectionInfo getParent() {
    return parent;
  }

}
