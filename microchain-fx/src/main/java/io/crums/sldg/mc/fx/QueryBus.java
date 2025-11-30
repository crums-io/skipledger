/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.sql.DataSource;

import io.crums.util.TaskStack;

/**
 * Query result cache. This class intermediates the construction
 * of objects from SQL queries and their posting on the UI thread.
 * 
 * <h2>First Usecase</h2>
 * <p>
 * On selecting any item in the list of database tables in the TOC view,
 * (a {@code ListView} on the left-side of split pane) a dynamically generated 
 * sampling of the selected table is displayed in a {@code TableView} in the
 * right-side of the split pane.
 * </p>
 * 
 * @param <T>  type of object constructed from queries
 * 
 * @see #QueryBus(DataSource, Function)
 * @see FxFunctorBus
 */
class QueryBus<T> extends FxFunctorBus<String, T> {
  
  
  private static class QueryFunction<T> implements Function<String, T> {
    
    private final DataSource dataSource;
    private final Function<ResultSet, T> factory;
    
    QueryFunction(DataSource dataSource, Function<ResultSet, T> factory) {
      this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
      this.factory = Objects.requireNonNull(factory, "factory");
    }

    /** Only SELECT statements work. */
    @Override
    public T apply(String sql) {
      try (var closer = new TaskStack()) {
        
        var con = closer.push( dataSource.getConnection() );
        var stmt = closer.push( con.createStatement() );
        
        ResultSet result = closer.push( stmt.executeQuery(sql) );
        return factory.apply(result);
        
      } catch (SQLException sx) {
        throw new SqlAppException(sx);
      }
    }
  }
  
  

  /**
   * Full constructor.
   * 
   * @param dataSource  data source (hikari)
   * @param factory     relational object mapping factory 
   */
  public QueryBus(DataSource dataSource, Function<ResultSet, T> factory) {
    super(new QueryFunction<>(dataSource, factory));
  }
  
  
  /**
   * {@inheritDoc}
   * 
   * @param sql SQL query
   */
  @Override
  public void get(String sql, Consumer<T> callback) {
    super.get(sql, callback);
  }
  
  

}
























