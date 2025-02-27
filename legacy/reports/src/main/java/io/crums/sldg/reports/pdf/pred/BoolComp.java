package io.crums.sldg.reports.pdf.pred;

import java.util.NoSuchElementException;

import io.crums.util.PrimitiveComparator;


/**
 * Boolean comparison operator.
 */
public enum BoolComp {
  EQ("="),
  GT(">"),
  GTE(">="),
  LT("<"),
  LTE("<=");


  
  private final String symbol;
  
  private BoolComp(String symbol)  {
    this.symbol = symbol;
  }
  
  public String symbol() {
    return symbol;
  }
  
  public boolean test(Number lhs, Number rhs) {
    int comp = PrimitiveComparator.INSTANCE.compare(lhs, rhs);
    return evalComp(comp);
  }
  
  
  private boolean evalComp(int comp) {
    switch (this) {
    case EQ:  return comp == 0;
    case GT:  return comp > 0;
    case GTE: return comp >= 0;
    case LT:  return comp < 0;
    case LTE: return comp <= 0;
    default:
      throw new RuntimeException("unaccounted enum: " + this);
    }
  }
  
  public <T extends Comparable<T>> boolean test(T left, T right) {
    int comp = left.compareTo(right);
    return evalComp(comp);
  }
  
  
  
  public static BoolComp forSymbol(String symbol) throws NoSuchElementException {
    for (var op : values())
      if (op.symbol.equals(symbol))
        return op;
    throw new NoSuchElementException(symbol);
  }
}








