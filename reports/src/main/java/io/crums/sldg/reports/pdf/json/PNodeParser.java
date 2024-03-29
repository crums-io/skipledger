/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.util.Objects;
import java.util.function.Predicate;

import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.PNode.Op;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * Predicate tree parser base class.
 * <p>
 * Our predicate parsing is complicated by the fact that some predicates
 * support user input. They allow predicates of the form {@code columnValue >= ?} (in pseudo
 * code) where the RHS '?' value is supplied as input.
 * </p> <p>
 * So the way this works right now, some numbers on the RHS of the predicate
 * structure (the '?' in the pseudo code above) are implemented by a special {@code Number}
 * class {@linkplain NumberArg}.
 * </p>
 * <h2>Use {@code EditableRefContext} on Write-path</h2>
 * <p>
 * On the <em>write</em>-path, the parser recognizes this special {@code Number} implementation
 * and populates (stuffs) the supplied {@linkplain EditableRefContext} with {@code NumberArg}s
 * (along with the meta info they carry). (The JSON serialization of the {@code RefContext} is
 * handled by a separate parser. Since the JSON in-memory object model is built from the
 * bottom up, before it gets serialized, this shouldn't present a problem.)
 * </p><p>
 * Recall our two use cases of {@linkplain PNode} involve a predicate tree (for columns) whose leaves
 * are another type of predicate tree (for cell values). It is this latter predicate tree that
 * takes wild card inputs (currently no use case for wild card '?' column numbers). But since
 * the former tree embeds the latter in its leaves, both parsers need an editable context.
 * </p>
 * 
 * @see SourceRowPredicateTreeParser
 */
public abstract class PNodeParser<T, U extends Predicate<T>>
    implements ContextedParser<PNode<T, U>> {

  public final static String OP = "op";
  
  public final static String AND = Op.AND.name();
  public final static String OR = Op.OR.name();

  public final static String SUB = "sub";
  
  
  
  
  protected final RefContext defaultContext;
  

  /** Constructs an instance with an empty, read-only context. */
  public PNodeParser() {
    this.defaultContext = RefContext.EMPTY;
  }
  
  /** @param context the default context (not null) */
  public PNodeParser(RefContext context) {
    this.defaultContext = Objects.requireNonNull(context, "null ref context");
  }
  
  /** @return the instance set at construction (may be empty) */
  @Override
  public RefContext defaultContext() {
    return defaultContext;
  }
  


  

  
  
  

  @Override
  public final JSONObject injectEntity(
      PNode<T, U> pNode, JSONObject jObj, RefContext context) {
    
    if (pNode.isLeaf())
      injectLeaf((PNode.Leaf<T, U>) pNode, jObj, context);
    else
      injectBranch(pNode, jObj, context);
      
    return jObj;
  }
  
  
  /**
   * Injects a leaf node in the JSON representation of the tree.
   * 
   * @param pNode   the leaf node
   * @param jObj    the JSON object the leaf node is directly represented in
   * @param context used by downstream parsers
   * 
   * @see #toLeaf(JSONObject, RefContext)
   */
  protected abstract void injectLeaf(
      PNode.Leaf<T, U> pNode, JSONObject jObj, RefContext context);
  
  
  /** No reason to override. */
  private void injectBranch(
      PNode<T, U> pNode, JSONObject jObj, RefContext context) {
    var branch = (PNode.Branch<T, U>) pNode;
    jObj.put(OP, branch.op().name());
    jObj.put(SUB, toJsonArray(branch.getChildren(), context));
  }
  
  
  
  
  
  
  @Override
  public final PNode<T, U> toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    return isBranch(jObj) ? toBranch(jObj, context) : toLeaf(jObj, context);
  }
  

  
  private PNode<T, U> toBranch(JSONObject jObj, RefContext context) {
    Op op = branchOp(jObj);
    
    var jSubs = JsonUtils.getJsonArray(jObj, SUB, true);
    if (jSubs.size() < 2)
      throw new JsonParsingException("too few (" + jSubs.size() + ") \"" + SUB + "\" components");
    
    var subNodes = toEntityList(jSubs, context);
    return PNode.branch(subNodes, op);
  }
  
  private boolean isBranch(JSONObject jObj) {
    String op = JsonUtils.getString(jObj, OP, false);
    return op != null && (AND.equals(op) || OR.equals(op));
  }
  
  

  /**
   * Deserializes and returns leaf node instance from the given JSON object.
   * 
   * @return a {@linkplain PNode.Leaf} instance
   * 
   * @see #injectLeaf(PNode.Leaf, JSONObject, RefContext)
   */
  protected abstract PNode<T, U> toLeaf(JSONObject jObj, RefContext context);
  
  
  
  private PNode.Op branchOp(JSONObject jObj) throws JsonParsingException {
    var opStr = JsonUtils.getString(jObj, OP, true);
    try {
      return Op.valueOf(opStr);
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException("illegal op: " + opStr);
    }
  }
  
  
}
