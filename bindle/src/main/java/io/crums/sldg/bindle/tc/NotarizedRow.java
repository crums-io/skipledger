/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle.tc;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.io.buffer.BufferUtils;
import io.crums.tc.CargoProof;
import io.crums.tc.Crum;
import io.crums.tc.Crumtrail;

/**
 * A hash proof asserting how a referenced cargo hash (in a timechain block)
 * is derived from the witnessed hash (crum). The witnessed hash is the
 * commitment hash of another ledger (at some row no.) In most cases (when
 * more than one hash was witnessed at the timechain in that time-block),
 * the hash proof is a Merkle proof. Presently, however, the Merkle tree
 * library does not cover an important corner case: the singleton (one-element)
 * set. Until that lib is fixed, the cargo hash proof must be custom-handled for
 * the singeton case (as done here).
 */
public class NotarizedRow implements Serial {
  
  
  final static int MIN_BYTE_SIZE = Crum.DATA_SIZE + 8;
  
  private final long rowNo;
  
  private final CargoProof cargoProof;
  
  private final Crum crum;
  
  
  
  public NotarizedRow(long rowNo, CargoProof cargoProof) {
    this(rowNo, cargoProof, null);
    Objects.requireNonNull(cargoProof, "null cargoProof");
  }
  
  
  public NotarizedRow(long rowNo, Crum crum) {
    this(rowNo, null, crum);
    Objects.requireNonNull(crum, "null crum");
  }
  
  
  private NotarizedRow(long rowNo, CargoProof cargoProof, Crum crum) {
    this.rowNo = rowNo;
    this.cargoProof = cargoProof;
    this.crum = crum;
    
    if (rowNo <= 0L)
      throw new IllegalArgumentException("rowNo " + rowNo);
  }
  
  
  
  /**
   * Returns the row number witnessed.
   * 
   * @return &ge; 1
   */
  public final long rowNo() {
    return rowNo;
  }
  
  
  /**
   * Returns the hash of the row witnessed.
   * 
   * @return {@code crum().hash()}
   */
  public final ByteBuffer rowHash() {
    return crum().hash();
  }
  
  
  /**
   * Returns the witness time in UTC millis.
   * 
   * @return {@code crum().utc()}
   */
  public final long utc() {
    return crum().utc();
  }
  
  
  /**
   * Instances are equal if they have the same row number
   * and cargo hash.
   */
  @Override
  public final boolean equals(Object o) {
    return
        o == this ||
        o instanceof NotarizedRow other &&
        rowNo == other.rowNo  &&
        other.cargoHash().equals(cargoHash());
  }
  

  /**
   * Returns a cheap hashCode (determined solely by row number).
   */
  @Override
  public final int hashCode() {
    return Long.hashCode(rowNo);
  }
  
  
  public final ByteBuffer cargoHash() {
    byte[] hash = crum == null ? cargoProof.rootHash() : crum.witnessHash();
    return ByteBuffer.wrap(hash).asReadOnlyBuffer();
  }
  
  
  public final int crumCount() {
    return crum == null ? cargoProof.leafCount() : 1;
  }
  
  
  
  public final Optional<CargoProof> merkleProof() {
    return Optional.ofNullable(cargoProof);
  }
  
  
  public final Crum crum() {
    return crum == null ? cargoProof.crum() : crum;
  }


  @Override
  public int serialSize() {
    return 12 + (crum == null ? cargoProof.serialSize() : crum.serialSize());
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    out.putLong(rowNo);
    int cc = crumCount();
    if (cc == 1)
      crum.writeTo(out.putInt(cc));
    else
      cargoProof.writeTo(out);
    return out;
  }
  
  
  
  public static NotarizedRow load(ByteBuffer in)
      throws SerialFormatException, BufferUnderflowException {
    long rowNo = in.getLong();
    if (rowNo <= 0)
      throw new SerialFormatException(
          "read row no. %d at offset %d from %s"
          .formatted(rowNo, in.position() - 8, in));
    int cc = in.getInt();
    if (cc <= 0)
      throw new SerialFormatException(
          "read crum count %d at offset %d from %s"
          .formatted(cc, in.position() - 4, in));
    if (cc == 1) {
      var cdata = BufferUtils.slice(in, Crum.DATA_SIZE);
      Crum crum = new Crum(cdata);
      return new NotarizedRow(rowNo, crum);
    } else {
      var cargoProof = CargoProof.load(in, cc);
      return new NotarizedRow(rowNo, cargoProof);
    }
  }
  
  
  
  public static NotarizedRow forWitnessedRow(long rowNo, Crumtrail trail) {
    return trail.isMerkled() ?
        new NotarizedRow(rowNo, trail.asMerkleTrail().cargoProof()) :
          new NotarizedRow(rowNo, trail.crum());
  }

}

















