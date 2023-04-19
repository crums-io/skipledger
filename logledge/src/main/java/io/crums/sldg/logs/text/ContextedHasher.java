/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.ColumnValue;

/**
 * Insead of overriding {@linkplain StateHasher}, you implement a
 * {@linkplain Context}. I want to mixin capabilities, and class hierarchies
 * get in the way. We'll see if this pans out.
 */
public class ContextedHasher extends StateHasher {
  
  /**
   * Overridable methods and callbacks defined in {@linkplain StateHasher}
   * defined here as a sort of SPI. You can also implement your behaviorial
   * side effects here. Can be stateful (e.g. {@linkplain #stopPlay()}).
   * 
   * @see ContextArray
   */
  public interface Context {
    /**
     * Invoked once at the beginning of each {@linkplain StateHasher#play(java.nio.channels.ReadableByteChannel, State) play}.
     * Use this for instance initialization. (This method is actually invoked
     * thru {@linkplain StateHasher#lineBufferSize()} which is only called
     * once per run.)
     */
    default void init() {  }
    /**
     * Controls the maximum number of bytes in a line. Defaults to 8k.
     * Invoked once per {@linkplain StateHasher#play(java.nio.channels.ReadableByteChannel, State)
     * play}.
     */
    default int lineBufferSize() {
      return 8192;
    }
    /** Do empty lines count? Defaults to {@code false}. */
    default boolean allowEmptyLines() {
      return false;
    }
    /** Should the parsing (play) stop? Defaults to {@code false}. */
    default boolean stopPlay() {
      return false;
    }
    /** Observes the row about to be ledgered. Defaults to noop. */
    default void observeRow(
        HashFrontier preFrontier, List<ColumnValue> cols,
        long offset, long endOffset, long lineNo)
            throws IOException {  }
    /** Observes the ledger line (it's hash in the skip ledger). Defaults to noop. */
    default void observeLedgeredLine(Fro frontier, long offset)
        throws IOException {  }
    /**
     * Returns the next saved state ahead of row number {@code rn}, if any;
     * {@code state}, otherwise. Defaults to returning the given fallback.
     * 
     * @param state fallback state
     * @param rn    target row number (&gt; {@code state.rowNumber()})
     * @return      next state with row number &ge; {@code state.rowNumber()}
     *              <em>and</em> &lt; {@code rn}
     */
    default State nextStateAhead(State state, long rn) throws IOException {
      return state;
    }
    /** Observes the end state (of play). Defaults to noop. */
    default void observeEndState(Fro fro) throws IOException {  }
  }
  
  
  
  protected final Context context;
  

  /**
   * Promotion constructor.
   * 
   * @param promote
   */
  public ContextedHasher(StateHasher promote, Context... context) {
    super(promote);
    this.context =
        context.length == 1 ?
            Objects.requireNonNull(context[0], "null context") :
              new ContextArray(context);
  }
  
  
  

  @Override
  protected int lineBufferSize() {
    context.init();
    return context.lineBufferSize();
  }

  @Override
  protected void observeRow(HashFrontier preFrontier, List<ColumnValue> cols,
      long offset, long endOffset, long lineNo)
          throws IOException {
    context.observeRow(preFrontier, cols, offset, endOffset, lineNo);
  }

  
  @Override
  protected boolean allowEmptyLines() {
    return context.allowEmptyLines();
  }

  @Override
  protected void observeLedgeredLine(Fro fro, long offset) throws IOException {
    context.observeLedgeredLine(fro, offset);
  }

  @Override
  protected boolean stopPlay() {
    return context.stopPlay();
  }

  @Override
  protected void observeEndState(Fro fro) throws IOException {
    context.observeEndState(fro);
  }
  
  
  
  
  @Override
  protected State nextStateAhead(State state, long rn) throws IOException {
    return context.nextStateAhead(state, rn);
  }




  public ContextedHasher appendContext(Context nextCtx) {
    Objects.requireNonNull(nextCtx, "null nextCtx");
    ContextArray ctx =
        this.context instanceof ContextArray ctxArray ?
            new ContextArray(ctxArray, nextCtx) :
              new ContextArray(this.context, nextCtx);
    return new ContextedHasher(this, ctx);
  }


  /**
   * Utility for mashing / queuing a bunch of contexts together.
   */
  public static class ContextArray implements Context {
    
    private final Context[] array;
    private final boolean allowEmptyLines;
    
    /**
     * @param array not empty, not null
     */
    public ContextArray(Context... array) {
      final int len = array.length;
      if (len == 0)
        throw new IllegalArgumentException("empty array");
      
      this.array = new Context[len];
      for (int i = len; i-- > 0;)
        this.array[i] = Objects.requireNonNull(array[i], "at index " + i);
      this.allowEmptyLines = allowEL(array);
    }
    
    
    
    
    public ContextArray(ContextArray copy, Context... array) {
      int clen = copy.array.length;
      
      this.array = new Context[clen + array.length];
      for (int index = 0; index < clen; ++index)
        this.array[index] = copy.array[index];
      for (int index = 0; index < array.length; ++index)
        this.array[index + clen] = Objects.requireNonNull(array[index], "at index " + index);
      this.allowEmptyLines = copy.allowEmptyLines || allowEL(array);
    }
    
    
    
    private boolean allowEL(Context[] array) {
      int i = array.length;
      while (i-- > 0 && !array[i].allowEmptyLines());
      return i != -1;
    }
    
    
    
    
    
    
    /** Initialize first to last. */
    public void init() {
      for (var ctx : array)
        ctx.init();
    }

    /** The maximum any one wants. */
    @Override
    public int lineBufferSize() {
      return
          Stream.of(array).map(Context::lineBufferSize)
          .max((a, b) -> a.compareTo(b)).get();
    }

    /** Any one says we do, we do. */
    @Override
    public boolean allowEmptyLines() {
      return allowEmptyLines;
    }

    /** Any one says stop, we stop. */
    @Override
    public boolean stopPlay() {
      int i = array.length;
      while (i-- > 0 && !array[i].stopPlay());
      return i != -1;
    }
    
    

    /** <p> Returns the best state by chaining inputs to outputs.</p>{@inheritDoc} 
     * @throws IOException */
    @Override
    public State nextStateAhead(State state, long rn) throws IOException {
      for (int i = 0; i < array.length; ++i)
        state = array[i].nextStateAhead(state, rn);
      if (state.rowNumber() >= rn)
        throw new RuntimeException(
            "state row number >= rn %d; state: %s"
            .formatted(rn, state.toString()));
      return state;
    }

    /** Observes first to last. 
     * @throws IOException */
    @Override
    public void observeRow(
        HashFrontier preFrontier, List<ColumnValue> cols,
        long offset, long endOffset, long lineNo)
            throws IOException {
      
      for (int i = 0; i < array.length; ++i)
        array[i].observeRow(
            preFrontier, cols, offset, endOffset, lineNo);
    }

    /** Observes first to last. 
     * @throws IOException */
    @Override
    public void observeLedgeredLine(Fro fro, long offset) throws IOException {
      
      for (int i = 0; i < array.length; ++i)
        array[i].observeLedgeredLine(fro, offset);
    }
    
    
    @Override
    public void observeEndState(Fro fro) throws IOException {
      for (int i = 0; i < array.length; ++i)
        array[i].observeEndState(fro);
    }
    
  }
  
  
  
  
  
  
  
  
  
 
}
