/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import javafx.application.Platform;

/**
 * JavaFX functor cache and bus. This class intermediates the construction
 * (retrieval) of expensive (time-consuming) objects and their posting on the
 * UI thread.
 * 
 * <h2>Purpose</h2>
 * <p>
 * Since the UI thread must not block, potentially time-consuming operations
 * (functions) must be dispatched to worker threads; state changes in the UI
 * are, in turn, communicated via {@linkplain Platform#runLater(Runnable)}.
 * It's the user who triggers these dispatches (thru the UI thread),
 * and in many cases, they may trigger multiple dispatches before any complete.
 * This class aids implementing these requirements in 3 ways:
 * </p>
 * <ol>
 * <li>
 * By asynchronously invoking the expensive function and returning its results
 * thru a user-supplied callback invoked from the UI thread.
 * </li><li>
 * By caching the results of already processed arguments.
 * </li><li>
 * By ensuring that no 2 threads race to execute the function using the same
 * arguments.
 * </li>
 * </ol> 
 * <p>
 * These properties are designed to ensure the application remains reponsive
 * no matter how busily a user selects or clicks UI elements, especially if
 * the UI elements that trigger are many (e.g. from a list or table view).
 * </p>
 * 
 * @param <K> the function's input type (considered a key to the cache);
 *            must be a <em>value</em> class and have proper
 *            {@linkplain #hashCode()}/{@linkplain #equals(Object)} semantics
 *            
 * @param <T> the function's return type
 * 
 * @see #get(Object, Consumer)
 */
public class FxFunctorBus<K, T> {
  
  private static class Holder<T> {
    T value;
  }
  
  
  private final Map<K, Holder<T>> cache = new HashMap<>();
  
  private final Function<K, T> factory;

  
  /**
   * Full constructor.
   * 
   * @param factory     function / factory, invoked at most once per key
   */
  public FxFunctorBus(Function<K, T> factory) {
    this.factory = Objects.requireNonNull(factory, "null factory");
  }
  
  
  
  /**
   * Retrieves the object associated with the given key and delivers
   * it thru the given callback. If the object (type {@code<T>}) exists
   * in cache, then callback is invoked immediately (before returning from
   * this method); otherwise, the function / factory (set at construction)
   * is invoked asynchronously, and the callback is invoked at a future
   * time.
   * <p>
   * This class is itself thread safe, however <em>this method is
   * expected to be called from the UI thread.</em>
   * No effort is made to enforce this behavior.
   * </p>
   * <h4>Edge Case Behavior</h4>
   * <p>
   * If this method is invoked with the same key quickly in succession,
   * i.e. before the first invocation's callback function has yet returned,
   * then only the first callback function is ever called. If the same callbacks
   * are used for the same key, then this behavior is, in fact, desirable.
   * </p>
   * 
   * @param key         the function / factory argument
   * @param callback    invoked on the UI thread using
   *                    {@linkplain Platform#runLater(Runnable)} typically
   *                    manipulating / changing some UI element
   */
  public void get(K key, Consumer<T> callback) {
    
    Objects.requireNonNull(callback, "null callback argument");
    
    final T value;
    final boolean alreadyRequested;
    
    synchronized (cache) {
      var holder = cache.get(key);
      if (holder == null) {
        cache.put(key, new Holder<>());
        alreadyRequested = false;
        value = null;
      } else {
        alreadyRequested = true;
        value = holder.value;
      }
    }
    
    if (alreadyRequested) {
      if (value != null)
        callback.accept(value);
      return;
    }
    
    Thread.ofVirtual().start(() -> buildAndCall(key, callback));
    
  }
  
  
  private void buildAndCall(K key, Consumer<T> callback) {
    
    T value = null;
    
    try {
      
      value = factory.apply(key);
      
    } finally {
      synchronized (cache) {
        if (value == null)
          cache.remove(key);
        else
          cache.get(key).value = value;
      }
    }
    
    final T cbVal = value;
    Platform.runLater(() -> callback.accept(cbVal));
  }

}























