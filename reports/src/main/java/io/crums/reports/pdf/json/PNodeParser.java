/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import io.crums.reports.pdf.model.NumberArg;
import io.crums.reports.pdf.model.pred.PNode;
import io.crums.reports.pdf.model.pred.PNode.Op;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
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
 * <h3>Use {@code EditableRefContext} on Write-path</h3>
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
 * <h3>Read-path</h3>
 * <p>
 * On the <em>read</em>-path, this parser <em>expects</em> the {@code RefContext} instance to
 * be pre-populated with <em>dynamic</em> (usually user) input. How that's achieved
 * (how it's pre-populate) is not <em>this</em> parser's concern.
 * </p>
 * 
 * @see CellPredicateParser
 * @see ColumnPredicateParser
 */
public abstract class PNodeParser<T, U extends Predicate<T>>
    implements ContextedParser<PNode<T, U>> {

  public final static String OP = "op";
  
  public final static String AND = Op.AND.name();
  public final static String OR = Op.OR.name();

  public final static String SUB = "sub";
  
  
  
  
  protected final RefContext defaultContext;
  

  /** Constructs an instance with an empty, read-only context. */
  PNodeParser() {
    this.defaultContext = RefContext.EMPTY;
  }
  
  /** @param context the default context (not null) */
  PNodeParser(RefContext context) {
    this.defaultContext = Objects.requireNonNull(context, "null ref context");
  }
  
  /** @return the instance set at construction (may be empty) */
  @Override
  public RefContext defaultContext() {
    return defaultContext;
  }
  
  /** Returns the default ref context, if it's editable. */
  public Optional<EditableRefContext> getEditableContext() {
    return defaultContext instanceof EditableRefContext e ?
        Optional.of(e) : Optional.empty();
  }
  


  /**
   * Uses an editable ref context. <em>Not an interface method.</em>
   */
  public JSONObject toJsonObject(
      PNode<T, U> pNode, EditableRefContext context) {
    return injectEntity(pNode, new JSONObject(), context);
  }
  

  
  @Override
  public final JSONArray toJsonArray(List<PNode<T, U>> pNodes, RefContext context) {
    return toJsonArray(pNodes, toEditableContext(context));
  }
  
  
  public final JSONArray toJsonArray(List<PNode<T, U>> pNodes, EditableRefContext context) {
    var jArray = new JSONArray(pNodes.size());
    pNodes.forEach(e -> jArray.add(toJsonObject(e, context)));
    return jArray;
  }
  

  @Override
  public final JSONObject injectEntity(
      PNode<T, U> pNode, JSONObject jObj, RefContext context) {
    return injectEntity(pNode, jObj, toEditableContext(context));
  }

  
  private EditableRefContext toEditableContext(RefContext context)
      throws IllegalArgumentException {
    
    if (context instanceof EditableRefContext e)
      return e;
    throw new IllegalArgumentException(
        (context == defaultContext ? "default " : "") +
        "ref context not editable: " + context);
  }
  
  
  public final JSONObject injectEntity(
      PNode<T, U> pNode, JSONObject jObj, EditableRefContext context) {
    
    if (pNode.isLeaf())
      injectLeaf((PNode.Leaf<T, U>) pNode, jObj, context);
    else
      injectBranch(pNode, jObj, context);
      
    return jObj;
  }
  
  
  
  protected abstract void injectLeaf(
      PNode.Leaf<T, U> pNode, JSONObject jObj, EditableRefContext context);
  
  
  /** No reason to override. */
  private void injectBranch(
      PNode<T, U> pNode, JSONObject jObj, EditableRefContext context) {
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
