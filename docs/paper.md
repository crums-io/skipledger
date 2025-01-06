
## Skip Ledger: a cryptographic commitment scheme for ledgers
##### DRAFT
<p align="center"> Babak Farhang <br/> crums.io  <br/> January 2025</p>

<br/>

## Abstract

This paper introduces Skip Ledger, a novel commitment scheme designed for evolving, append-only lists,
or simply ledgers.
Unlike traditional hash chains, skip ledger embeds a binary hash tree structure in the
calculation of each row's hash, enabling logarithmically succinct proofs of inclusion and order for any set of
ledger rows. The hash structure enabling efficient partial reveals of ledger contents is described, then
formalized in pseudo-code by the defining a function that calculates a row's commitment hash, in terms of
both the row number and a user-defined function that returns the hash of the *contents* of a ledger row.
Besides enabling succint proofs of ledger row contents, the scheme's perhaps most notable property
is that it also enables succint proofs of the order of the commitments, themselves. The scheme's efficiency
in areas requiring audit, integrity, and non-repudiation are discussed and contrasted with other approaches.
The scheme's applicability to blockchain protocols (more efficient off-chain proofs) is noted. Finally,
the paper posits if blockchains can create tokens of value in their ledger entries, then so may private
ledgers.

<br/>

## Introduction

Ledgers are historical lists of things. Whether by markings on a stick, carvings in clay, or ink on paper,
ledgers have always recorded human activity. And while the technologies for maintaining the veracity of those
records have evolved, one principle has remained constant: the technology should make tampering with the records
difficult (dry clay is not editable, for example), so that tampered records are readily apparent. This then is
another modern take on those markings on a stick.

Traditional methods like hash chains[^1] provide strong security guarantees about order (which record
precedes another, for example), but suffer from ineffeciencies when it comes to *verifying* that
order. In particular, with most hash chains, verifying order requires reading the chain from the beginning.
Since hash chains are typically large, it's difficut to package succint off-chain proofs of order.

Skip ledger, however, enables succinct, off-chain constructions for proof of order. A sketch of the typical
usage model under the scheme goes something like this:

>  Given the commitment hash for block [*N*] (*N* > 0) establish that it encodes the contents (or commitment hash)
  of block [*n*] (0 < *n* ≤ *N*) in a succinct proof.

That is, if a user has reason to believe they know (or can trust) the commitment hash for block [*N*], then
they can be convinced about the contents of any block at or before *N* using a compact, off-chain hash proof.

After a brief review of prior related work, the scheme's data model (ledgers as append-only rows/blocks)
and commitment scheme are defined in terms of a user-defined `input_hash( n )` function that returns
the hash of the contents of the row numbered *n*, and the [fixed] skip ledger commitment hash function,
`row_hash( n )`. Next, the scheme's properties are enumerated (what qualifies it as a commitment scheme;
what makes skip ledger a useful primitive) with emphasis on the scheme's fundamental proof structure called
a *path*. Paths are to skip ledgers what Merkle proofs are to Merkle trees; but unlike Merkle proofs (which model
*fixed*, not append-only, collections), paths are composable from (and *de*-composable to) sub-paths. An informal
"mini-algebra" about the row no.s in the composition of paths is introduced to justify some of the claimed
properties. The *path* proof structure is only described in words, referencing only the already defined
`input_hash` and `row_hash` functions; pseudo code for the actual packaging of proofs (skip ledger paths)
is not included.

The `row_hash` function, as defined recursively in the pseudo code, is inefficient. The user-defined, ledger-specific
`input_hash` function too may be inefficient. The section on skip ledger chains outlines a simple
"memo-ization" techinique that allows a ledger owner to generate paths (succinct proofs of order and inclusion)
in constant time; other use cases that involve less frequent recordings of a "ledger"'s commitment hash
(where there are no immediate plans for revealing anything), use a succinct memo-ization structure (called *hash frontier*)
to efficiently calculate a row [commitment] hashes forward, in a "single-pass".

In the Discussion section, skip ledger is compared and contrasted with other approaches,
emphasizing its applicability in a range of use cases. A ledger (an append-only collection), after all, can model
many a thing: from log files, streams or sequences of "data frames", to journals, private ledgers, public ledgers,
blockchain, or other kinds of objects yet imagined. The scheme's general applicability and efficiency in the
areas of audit, proof of provenance, non-repudiation (etc.) are analyzed. The section closes with a hypothetical
warehouse ledger, using it to suggest that a private ledger (backed by business operations) can encode and expose
tokens of value much like blockchains do.


### Prior / Related Work

Both Merkle trees[^2][^3] and hash chains, while prominently featured in blockchain technology (Nakamoto, 2008)[^5],
have a rich history in cryptography and computer science.
They have been used for various purposes, including generating one-time keys in Lamport signatures[^4][^6], securing password storage (Stallings, 2018)[^7], and providing verifiable time stamps for digital documents (Haber & Stornetta, 1991)[^1].

Hash proof structures often mirror analogous, conventional data structures. Merkle trees, for example,
are structurally similar to binary trees; hash chains, are similar to linked lists. The word *mirror* here also conveys
the idea that the references in these hash proof structures point in the *opposite direction* of their conventional
counterparts. For example, the hash references (commonly call *hash pointers*) in a Merkle proof are constructed from the
bottom up , from leaf to root, rather than vice versa (as in conventional binary trees). Skip ledgers too
mirror a conventional type of data structure: the skip list (Pugh, 1990)[^8].

The use of skip list-like hash structures for cryptographic linking (and commitment) is not a new concept.
For example, the NIST Randomness Beacon (Kelsey, Brandão, Peralta, & Booth, 2019)[^9] records (commits) successive beacon
hashes in a skip list-like hash structure organized like the Gregorian calendar (year/month/day, etc.).
Although skip lists are mentioned or used in prior, related work, skip ledger offers a more concrete skip-list-based
commitment scheme that is applicable in more general settings.

<br/>

## Model

A ledger is modeled as an append-only list of rows (items), each row represented by
a hash value derived from the row's contents (a line of text, a row in a database, a "chunk"
of a stream, etc.) using a cryptographically strong hash function **H**.
In skip ledger terminology, this hash value is called the row's *input hash*. Additionally,
each skip ledger row has a computable *commitment hash*, which in skip ledger terminology
is just called the *row-hash*. That is, a row-hash at row *n* also stands in as a commitment hash
for the ledger when it had *n* rows.

Indeed this is an already familiar concept in blockchain[^5]: the hash of the *n*<sup>th</sup> block
is a commitment for the state of the entire chain when it had *n* blocks. There, each block is
linked to its predecessor using a special hash cell that simply records the hash of the predecessor
block. Under the skip ledger commitment scheme, each row's (block's) hash is computed as the
root of a binary hash tree over that and all previous rows in the ledger. This property, in turn, allows
one to efficiently reveal and prove (partial reveal) the contents of any row against *any* commitment
hash of the ledger after it had that row (block).

## Row Hash

Rows are numbered, starting from 1, monotonically increasing, with no gaps.
Every computed row hash encodes the hash of the preceding row,
but also, depending on the row number *n*, *k* many more hashes of predecessor rows, where
*k* is the number of times 2 divides *n*:

*k* = max { *i* ∈ ℕ<sub>0</sub> s.t. 2<sup>*i*</sup> | *n* }

Each row's hash "directly" encodes *k + 1* predecessor row hashes. The hashes referenced
are from rows numbered

*n* - 2<sup>0</sup>, *n* - 2<sup>1</sup>, *n* - 2<sup>2</sup>, .. , *n* - 2<sup>*k*</sup>

or more simply

*n* - 1, *n* - 2, *n* - 4, .. , *n* - 2<sup>*k*</sup>

The hash of row [*n*] is computed by hashing the concatenation of the row's *input hash* (computed from the source row)
together with the root hash of a Merkle tree constructed from the row hashes of the predecessor rows
numbered above.

The following pseudo code fleshes out a recursive definition for the row hash:

>
        
      // Strong cryptographic hash function. The hash of the empty [byte] sequence
      // H("") is _defined_ as a string of zero bits with the same length as H's
      // usual output. This sentinel value is denoted H_o below.
      //
      function H(m) :


      // Computes and returns the Merkle root hash over the given ordered list of hashes
      // using a strong cryptographic hash function. The algorithm here does not prepend
      // internal and leaf hash nodes (with 0 and 1): since the no. of hashes is predetermined
      // in this application, the so called "2nd-preimage attack" does not apply.
      // 
      function merkle_root(hashes) :
        if (length(hashes) == 0) return H_o             // (never reached in this pseudo code)
        if (length(hashes) == 1) return hashes[0]       // hash of singleton is self

        while (length(hashes) > 1) {
          new_hashes ← []
          for i ← 0 to length(hashes) - 1 step 2 {
            if (i + 1 < length(hashes))
              new_hashes.push( H(hashes[i] + hashes[i+1]) )
            else
              new_hashes.push( H(hashes[i]) )
          }
          hashes ← new_hashes
        }
        return hashes[0]

        
      // Computes and returns the hash of the *source* row in the ledger
      // numbered n (> 0). This may the "straight" hash of a byte sequence, or
      // a "composition" of the hashes of cell values in row [n].
      //
      function input_hash(n) :
        // .. compute hash of ledger source row (or read memo-ized result)
        return h
        



      // Recursive definition of a row's commitment hash (simply called row hash)
      //
      function row_hash(n) :
        // check for the sentinel row [0]
        if (n == 0)
          return H_o                      // (zeroed hash)
        
        pn ← pointer_nos(n)
        ptr_count = length(pn)
        prev_row_hashes ← [ptr_count]   // an array of hashes (or other container) with ptr_count-many slots
        for i ← 0 to ptr_count - 1 do
          prev_row_hashes[i] ← row_hash(pn[i])

        // below, '+' means concatenate
        return H( input_hash(n) + merke_root(prev_row_hashes) ) 








        // Returns the row no.s row n's hash is derived from. These are deterministically
        // set by row no. (unlike the randomization in the skip list search structure).
        //
        function pointer_nos(n) :
          count ← skip_count(n)
          PN ← [count]    // an array (or other container) with count-many slots
          for i ← 0 to count - 1 do
            PN[i] ← n - 2^i
          return PN


        // Returns the no. of skip [hash] pointers to previous rows for a given row no.
        // (determined by row no. alone).
        //
        function skip_count(n) :
          e ← trailing_zero_bits(n)    // no. of trailing zero bits in the base-2 representation of n
          return e + 1


Note, in practice a `row_hash` implementation will involve some form of
memo-ization (in memory, or on disk); otherwise, each access of `row_hash(n)`
will cost **O**(*n*) operations.

## Properties

The `row_hash` function above has sufficient properties to satisfy as a *commitment scheme*.

> A commitment scheme is a cryptographic primitive that allows one to commit to a chosen value (or chosen
> statement) while keeping it hidden to others, with the ability to reveal the committed value later.
<p align="right">Oded Goldreich (2001). Foundations of Cryptography: Volume 1, Basic Tools. Cambridge University Press.</p>

### Commitment
The hash of any row numbered *n* (see `row_hash`)  acts as a commitment to the contents
of a ledger with *n* rows.

### Binding
Once the hash of row *n* is published, it is computationally
difficult to change the contents of any row numbered *n* or lower without changing
the hash of row *n*. This follows from the fact that hash of every row always
depends on the hash of the immediate row before it.

### Hiding
The hash of a row numbered *n* does not reveal anything about the contents of the
ledger with *n* rows, even if *n* (the row no.) is revealed.

### Partial Reveal

The commitment scheme enables 2 layers of differential information disclosure:
general and specific.

#### *Path* (General)

Once the hash of row *n* is published (committed to),
the ledger owner may later reveal how the hash row *n* is derived from the
hash of any row numbered less than *n*. In skip ledger terminology, such
proofs are called *paths*. Since [the row] hashes don't leak information,
the only information conveyed is about the row numbers themselves.

**_Def._** The shortest path linking two numbered rows (the one with the fewest references to the hashes of
intervening rows) is called the *skip path* (for the numbered rows).

The next section explains the mechanics of paths using a particular example.

#### *State* (General)

Once the hash of row *n* is published, one may later
reveal (and prove) the number of rows in the ledger *n*. This is achieved using a *path*
(a hash proof) linking the hash of row [*n*] to the hash of the first row,
row [1]. For example, if *n* is 534, then the path contains information about
constructing the hashes of the following row no.s:

`[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 528, 532, 534]`
  
In skip ledger terminology, these are the path's "full" rows. From left to right,
the hash of each row is determined by

  1. The input hash (the hash of the source data) at that row no.
  2. A Merkle proof linking the hash of the row no. at the left of a no.
  to a Merkle root hash. (The first row [1] links to the sentinel
  row [0] whose hash is a string of zeroed bits.)
  3. The row's hash is the hash of the concatenation of the row's input hash
  (1) and the (link) Merkle root hash (2).

Since row [0], the sentinel row, is only referenced "directly" (via a row's link Merkle tree)
by rows numbered 2<sup>*k*</sup> (*k* = 1, 2, 3, ..), the structure of 
the hash proof reveals *n*, the number of rows in the ledger with the given commitment hash.

Note the above path is in fact a skip path: it is the shortest path that links row [1]
to row [*n*]. Since the row no.s in a skip path are predetermined by their beginning and ending
no.s, I'll denote this set of ordered numbers as `{1 → 534}`. Similary,

`{a → b → c} = {a → b} ∪ {b → c}`

shall denote the union of 2 skip paths. Paths and skip paths (their row no.s) have interesting
algebraic properties. In particular,

1. Every path is a composition of skip paths.
2. Every path at least includes the row no.s generated by the skip path with the same start and ending row no.s.

#### *Source row* (Specific)

Once the hash of row *n* is published (committed to),
the ledger owner may later reveal the contents of any row numbered less than or
equal to *n*. If the revealed row is numbered *r* (1 ≤ *r* ≤ *n*), then the
path shall include the row no.s `{1 → r → n}`.
The contents of the source row *r* can then be revealed the "usual way"
by matching the source row data with the input hash of the [skip ledger] row *r*.

## Other Properties

The properties enumerated above satisy the definition of a commitment scheme. This next set
of properties make the skip ledger a *useful* primitive. The focus here, is on the
properties of paths, the hash proof structures that enable partial reveals.

### *Logarithmic Succinctness*

The skip ledger paths linking any set of row no.s
are compact. The number of full [skip ledger] rows in any skip path is of order
**O** log *n* (where *n* is the *difference* of the last and first row no.s). Additionally, each
row hash in a skip path on average encodes log *n* many hash pointers to previous
rows: so, on average, the *Merkle tree* used to construct the hash of a full row
contains log *n* previous-row hashes as its leaves. Thus each *Merkle proof* linking
successive full rows in a skip path is of size **O** log log *n*. Multiplyiing
the average no. of full rows with the average size of each row's merkle proof
linking it to the predecessor row in the path, the binary size of a skip ledger path
is of order

  **O** (log log *n* ) log *n* ≅ **O** log *n*

The justification for the "near equivalence" to **O** log *n* is that the first
factor log log *n* grows extremely slowly and from a practical standpoint can be
expected to never exceed 6 (*n* = 2<sup>64</sup>).

In actuality, not all *link* proofs in the path structure are worth expressing as Merkle proofs:
for a row numbered *n*, if *n* is not a factor of 16, then simply listing (or otherwise
inferring) all the referenced row hashes (by row [*n*]) is more space efficient.

### *Ensemble Compressibility*

Since paths from the same ledger share common lineage (and therefore common hashes),
when archived together the paths take less space than they would take if
archived individually. This is because paths can be decomposed into (and recomposed
from) their parts.

### *Composition*

Some paths can be concatenated with other paths from the same ledger. In particular,
given two paths from the same ledger for row no.s `a`, `b`, and `c`

`{a → b}` and `{b → c}` (with `a < b < c   a,b,c ∈ ℕ`)

a new path from `a` to `c`, `{a → b → c}` can be composed from their "union" (concatenation).

Generally, given any two *overlapping* paths (from the same ledger)

`{a → c}` and `{b → d}`  with `a < b < c < d`, and `a,b,c,d ∈ ℕ`

∃ `b⁺ ∈ {a → c} ∩ {b → d}`, with `b ≤ b⁺ ≤ c`

frow which a new path

`{a → b⁺ → d}`

may be constructed.

### *Commit Provenance*

If a ledger owner periodically publishes their ledger's commitment hash, they can also
publish an *intermediate* commitment artifact, revealing the no. of rows in the ledger
when that commitment was made, as well as a succint proof linking the last commit to
the previous commit. This is achieved using skip ledger paths, of course. Since paths
are compact, a ledger owner may opt to publish a commitment as a path linking the
latest committed row to the previously commited row.

### *Ensemble Commit Update*

If paths from the same ledger are archived using properly
overlapping sub-paths, then the addition of a single path (from the highest numbered
row in the collection to a more recent committed row) updates
*all* the proofs (paths) in the archive to the path's highest numbered row. I.e. a
single piece of compact data updates all existing proofs to the hash of a more
recent commit.

<br/><br/>
This concludes the roundup of the properties of skip ledger proof structures.
Paths have other interesting properties worth exploring (the algebraic properties of their full
row no.s, for e.g.) but those are beyond the scope of this paper.


## Representation

In principle, a ledger managed under this commitment scheme can just record
the hash of the last row committed to an abstract skip ledger. In many niche cases this simple
representation is appropriate. For example, if the lines in a log file are modeled as rows
in a ledger and the log file is archived it for its *potential* evidentiary value, then
simply recording the skip ledger hash of the last line of the log file
may be appropriate. If log files are rolled, then the (hash) state of the ledger can also be rolled
from one log file to the next using a special compact structure called the *hash frontier*. However,
neither of these examples really concern this section.

What concerns us here is the data structures to use when it's time to partially reveal one
or more row values against a committed hash. In order to tackle this problem, let us consider
the case where we do not have advance knowledge about which row values
will be revealed (at which row no.s) but we need to be able to generate such proofs (paths)
quickly, at will. The `row_hash` function defined recursively above is immensely inefficient.
Some form of persistent memo-ization is necessary to speed it up.

### Skip Ledger Chain

One way to record skip ledger row hashes is to represent each row in fixed-size blocks.
If each block only recorded the row-hash (the output of the `row_hash` function), then an
efficient, memo-ized version of `row_hash` might be implemented using this sequence of blocks as
a caching layer. However, the construction of paths (the hash proofs linking numbered rows)
also requires the input-hash for the row (the ouput of the `input_hash` function
which computes the hash of the source row numbered *n*). Depending on the data source
this might also be an expensive, non-constant-time operation (finding the offset of a line no.
in a log file, for example). To further speed up the construction of paths
(and also to cover cases where the `input_hash` function is characteristically
slow), we also record (and memo-ize) the input-hash of the row in each block. So if both the
input- and row-hash functions both use SHA-256 (as used in the reference implementation[^10]), then
each block representing a skip ledger row takes 64 bytes in the skip ledger chain.

Using the chain, individual row information is available in near constant time, and
constructing paths takes **O** (log *d*)<sup>2</sup> block accesses (where *d* is the difference
of the highest and lowest row no.s in the path). Note, the more skip pointers a row no.
has (the more trailing zeroes in the row no.'s binary representation), the more expensive
it is to access linkage information for that row no., but importantly, the more likely that row is included
in a random path. An in-memory caching layer that prioritizes linkage information at row no.s with the most
skip pointers, can ameliorate this log-squared performance penalty where it hurts most.

#### Chain Integrity

This section concerns detecting data corruption in the chain's blocks. It does not
concern verifying that the committed source rows have not be tampered with. There are 2
classes of integrity-check. 

##### Start-to-Finish

As in most "cryptographic chains" the integrity of the chain is verified by re-playing its
blocks from start to finish. The source rows committed to the chain are not themselves verified in this
"integrity-check": rather the row-hash recorded in each successive block is verified to be
consistent with input-hash recorded in that block. On average, [the hash of] each skip ledger
row (block) encodes (references) 2 previous row-hashes. An in-memory structure (the *hash frontier*
previously mentioned in the context of rolling log files) obviates the need to read any block
more than once during a run of the integrity-check.


##### Sampled Integrity

Every time a path is constructed from a skip ledger chain's blocks, the *derived* hash of the last
numbered row in the path can be verified to match that recorded in the block at that row no.
(The reference implementation makes this check.) This way, every path constructed from
the chain, is also a sampled integrity-check: to the degree their skip path coordinates are
random (as in their `{a → b → c}` notation), the construction of paths over a chain can be
seen as random-sampled integrity-checks. Since paths are the scheme's bread and butter, the more used,
the less the need to verify the chain from start-to-finish.

#### Source Integrity

Another motivation behind including the input-hash in the chain's blocks is that it allows
us to distinguish chain corruption from *source* ledger corruption. The procedure for checking
source integrity is to simply verify the recorded input-hash in each block matches that generated
from the source ledger for each row no. from start-to-finish. This start-to-finish test, of course,
can only be reliably performed by the ledger owner. However, every time a source row is revealed
this check is also performed by the recipient of the revealed row. (As with paths, the
reference implementation verifies the source row data matches the input hash on packaging its
proofs.)

## Discussion

What to do with this primitive? Where to use it? This section compares and contrasts skip ledger
with other techniques and approaches.

### Comparison: Merkle Trees


|  | Skip Ledger | Merkle Tree | Comment |
| --- | --- | --- | --- |
| Logarithmically succinct | ✓ | ✓ | Both skip ledgers paths and Merkle proofs are **O** log in size. |
| Balanced hash tree | slightly less balanced | ✓ | Merkle trees are perfectly balanced, leading to slightly smaller proofs. |
| Commit provenance | ✓ (inherent in structure)| difficult to link historical roots efficiently | skip ledger's structure naturally links all historical states through the row hashes. Linking different Merkle roots requires additional structures like hash chains or skip ledgers themselves. |
| Incremental build / commit | ✓  (build-as-you-go) | requires rebuilding the tree, if more leaves are added | skip ledgers efficiently integrate new entries without the need for an explicit "build" or commitment stage.  |
| **Use Synopsis** |  |  |  |
| Suitability for fixed-size collections | less suitable | ✓ | Merkle trees are a better choice when modeling a collection that does not grow, because Merkle proofs are slightly more compact. |
| Suitability for append-only collections | ✓ (designed for this) |  |  |



### Application to Blockchain

In most blockchains each block records the hash of the predecessor block
in a specially reserved slot, as many bits wide as the hash function used requires. We'll examine how
the hash recorded in that slot can be repurposed to instead to use the skip ledger commitment scheme.

The gist of the strategy here is to identify and separate a block's application-specific data from
its hash linking mechanism, and instead of calculating a block's hash the "usual way",
use the hash of a block's "application-specific" data to compute a block's input-hash
(in skip ledger terms) and "redundantly" record the (structured) skip ledger hash of the block itself in the
the block's hash pointer slot. With proof-of-work chains like Bitcoin[^5], the hash-neutral
nonce field will continue to not figure in the calculation of a block's [input] hash (i.e. proof-of-work
does not contribute to the block's input hash); for chains operating under other consensus schemes, such as
proof-of-stake, it may be appropriate to include proof-of-consensus data in the calculation of the
input hash.

Note skip ledger can be adopted retroactively. That is, once the chain's `input_hash` function
is properly defined, the skip ledger commitment hash is computable for every existing block. The existing blocks
can be augumented using a parallel chain that only records the skip ledger commitment hash at each block no. At 32 bytes
overhead per block (using SHA-256), this would count as perhaps the smallest index built atop the chain. The
auxilliary parallel chain ends right before the block no. the commitment scheme is switched over to.

Starting from the block no. the scheme is adopted, blocks are referenced
by their skip ledger hash (instead of how block-hashes are presently calculated), which again, is
redundantly recorded in the block itself. The proof-of-work puzzle
(attempting to zero the hash of the block, computed the *old way*) remains unchanged.

#### Potential Advantages

Under skip ledger, any random existing block can quickly be verified to belong in the chain
(as represented by any of its recent blocks) in sublinear time. This, in turn, may aid in tracking,
verifying, or packaging off-chain proofs-of-provenance for a chain's tokens.


### Application to General Ledgers

Business ledgers are usually maintained in relational databases. Smaller businesses, on the other hand,
may simply maintain some of their "ledgers" in spreadsheets. Regardless where and how such ledgers
are kept, businesses can ensure (and prove) their ledger data have not been corrupted by periodically
committing to (recording) the hash of their ledgers.

Since many existing ledgers are in tabular form, the design of the `input_hash` function for such
use cases is also an area of study. Areas of focus include:

1. How to define a ledger using a single SQL query (a SELECT statement often joining data from multiple
tables for each logical row no.).
2. How to compute a ledger row's input-hash from the composition of its individual cell hashes so that
individual row cells may be redacted when revealing the contents of a row.
3. How to salt individual cell values so that their salted hashes resist rainbow attacks, frequency analysis, etc.
with negligible space overhead. The prototyped solution uses a secret ledger-wide pseudo random byte sequence
as seed to generate
    1. a row-number-specific pseudo-random salt (computed from the SHA-256 hash of the secret seed and row no.)
    2. a cell-specific pseudo-random salt (computed from the SHA-256 hash of the row-specific salt and the cell's index / column no.)
  
  Under the "table salt" scheme (3) , the salt for every cell value in the row is derivable from it the row-salt
  and the cell's index (in the row), but not the other way around. When no values are redacted, only the
  row-salt needs to be revealed in the proof; if any cell value *is redacted*, then the row-salt *must not* be revealed and
  every revealed cell value is accompanied by the cell-specific salt.

### Application to Data Streams

The commitment scheme finds use in the sharing and archival large chunks of data (which may contain sensitive,
or priviledged information) for non-repudiation purposes. For such use cases, it is merely enough to
calculate (and commit to) the [skip ledger commitment] hash of the last "row" or block in one pass.

For unstructured data streams, that is, when the commitment system's `input_hash` function has no prior
knowledge of the stream's format, one may simply choose an `input_hash` function that describes
the stream as fixed-size blocks. This may be appropriate for video streams, for example.

From a practical perspective it may suffice to record (publish) the
commitment hash of, say, every 1024<sup>th</sup> block in the stream. This allows any snippet
to be verified against the commitment hash (and *where* it occurs in the stream). If the stream is
timestamped, the proof might also provide guarantees about *when* the snippet occurred.


### Application: Timechain Protocol

Timechain[^11] is an experimental REST based protocol for witnessing hashes. Here, a skip ledger chain
models contiguous, equal-duration time-blocks since the chain's genesis. The chain's `input_hash`
function computes the Merkle root of the hashes witnessed in that time-block. Users are expected to retrieve
witness proofs promptly: the Merkle tree used to compute the block's `input_hash` is not kept indefinitely.

#### *Historical Note*

Earlier versions of the timechain did not use this commitment scheme. It developed the other
way around. The design of skip ledger was first motivated by considering how best to construct
the hash of an evolving ledger to be witnessed by the (old) timechain.


### Proposition: *Every* Receipt is Recorded In Some Ledger

Every *modern* receipt is recorded in one or more ledgers. Gone are the days when an official
document (receipt) might merely be imprinted with the King's seal and not be
recorded anywhere else. If you can find exceptions to this rule, then consider it a definition.

If the *existing* ledger[s] that receipt came from is annotated with skip ledger
commitment data (proving at what row no.[s] the legered receipt is recorded in), then the
receipt reveals a commitment hash for its contents that can be efficiently linked to future
commitment hashes of the ledger[s] the receipt was recorded in.


### Comparison: Blockchain

Blockchains also model ledgers. Since going mainstream, blockchain is now a toolkit
many a cutting edge application should consider. Here the tradeoffs of the blockchain
approach versus managing privately controlled ledgers maintained under the skip ledger
scheme are examined at high level. But note, we're examining what is possible *in theory*: for example,
if a blockchain (which *is* a ledger) can create tokens of value in its entries, then
so too may privately controlled ledgers (backed by business processes operating under
conventional legal regimes) model tokens of value in their entries.

The table below contrasts the 2 approaches.


|  | Skip Ledger | Blockchain | Comment |
| --- | --- | --- | --- |
| Tamper proof | ✓ | ✓ | Both approaches ensure data integrity and guard against data corruption. Skip ledger, however, does not rely on blockchain infrastructure. |
| Governance | private | by consensus |  |
| Legal Regime | contract / judicial | code-as-law (mostly) |  |
| Decentralized |  | ✓ |  |
| Ledger Rules | by convention / contract | by code |  |
| Backward-compatibile | ✓ |  | Skip ledgers are based on *existing* bookkeeping ledgers |
| Easy to Model / Deploy | ✓ |  | Conventional ledgers operate in more controlled, less adversial environments than blockchains do. |
| Off-chain Verification | ✓ (by design) |  | Although a skip ledger based application may expose its rows (blocks) as a "commitment chain", as, for example, the [timechain protocol](#application-timechain-protocol) does, the chain's only function (as far as skip ledger is concerned) is to efficiently generate *off-chain* proofs of entry and/or commitment. The scheme itself does not assume a chain. |
| Accountability | ✓ |  | Conventional ledgers committed under the skip ledger scheme are more accountable than decentralized blockchains. |
| Revisions / Corrections | ✓ |  | Conventional ledgers model corrections to old entries with *new* "correction" entries (old entries are *never* be edited, of course). By design, entries in blockchains are *final*. |
| Easy to Evolve |  ✓ |  | Conventional ledgers are easier to evolve than blockchains. In fact, skip ledger can *aid* in the *evolution* of a ledger: for example, back-end database schema changes can be verified not to break old commitments. |
| Tokenizable parts | hypothesized | ✓ |  |


If blockchains can create tokens of value, can conventional ledgers too design and package tokens of value in their entries?
Let us explore this issue using a perhaps contrived, over-simplified example.

#### Warehouse Ledger: a Hypothetical Use Case

A warehouse exposes its inventory as a public, mostly obfuscated ledger. At first, there are 4 types of record (row) in this ledger:

1. Item entering (stocked in) warehouse.
2. Item exiting warehouse.
3. Stocked item changing hands (ownership).
4. Correction to an existing record.

A live view of its commitments, in the form of an opaque skip ledger chain, is also made public. The warehouse only dispenses
unredacted receipts to the priviledged 3rd parties it transacts with. Type (1) or type (3) receipts from this ledger may
function as proofs of ownership of items in stock. Entries in conventional ledgers often reference
records in 3rd party ledgers: for example, ownership-related entries in the warehouse ledger likely reference 3rd party records.
So too are the shipping-related entries in type (1) and type (2) records.

Note, regardless *how* and *where* an item's change-of-ownership is first recorded, the fact the warehouse provides
verifiable receipts for when warehoused stock has changed hands, makes its *in-stock* receipts valuable. An extreme example
of the *how* might be by a legal judgement against the current owner; the *happy path*, of course, is where both parties
are in agreement (warehoused items sold or bartered).

Business is good at the warehouse. After a while, managers notice some customers are using their
warehouse receipts as collateral for loans. They decide this is a good thing, but going forward, in addition to recording
ownership, the warehouse ledger will also allow legitimate parties to also record contractual encumberances on ownership.
Accordingly, the warehouse introduces a new record type (call it type 3a) for recording that ownership in an already stocked item has become encumbered.


### Closing Remarks

Skip ledger is a powerful addition to the toolbox. A number of implementation-specific engineering details not
covered in this paper are further explored in the reference implementations[^10][^11][^12]. For the most part, these concern realizing
efficiencies by developing a uniform set of tools and formats that both ledger owners and users (receipt recipients) know how to
use in very general and broad settings. In particular, a binary "archive" format for bundling ledger proofs is
prototyped. Because ledger entries often reference entries in *other* ledgers each archive (called a *morsel*)
packages proofs from *multiple* ledgers. For example, morsels package timechains (along with their witness proofs) just
like any other ledger.


### References

[^1]: Haber, S., Stornetta, W.S. (1991). *How to time-stamp a digital document.* Journal of Cryptology 3, 99–111
[^2]: Merkle, R.C.  (1978). *Secure communications over insecure channels.* Communications of the ACM, 21(4)
[^3]: Merkle, R.C. (1982). *Method of providing digital signatures.* US patent 4309569
[^4]: Lamport, L. (1981). *Password Authentication with Insecure Communication.* Communications of the ACM, 24(11)
[^5]: Nakamoto, S. (2008). *Bitcoin: A Peer-to-Peer Electronic Cash System,* https://bitcoin.org/bitcoin.pdf
[^6]: Lamport, L. (1979). *Constructing Digital Signatures from a One Way Function.* SRI International
[^7]: Stallings, W. (2018). *Effective cybersecurity: a guide to using best practices and standards.* Addison-Wesley Professional.
[^8]: Pugh, W. (1990). *Skip lists: a probabilistic alternative to balanced trees.* Communications of the ACM, 33(6), 668-676.
[^9]: Kelsey, J., Brandão, L. T., Peralta, R., & Booth, H. (2019). *A reference for randomness beacons: Format and protocol version 2* (No. NIST Internal or Interagency Report (NISTIR) 8213 (Draft)). National Institute of Standards and Technology.
[^10]: Skip ledger reference implementation https://github.com/crums-io/skipledger
[^11]: Timechain protocol reference / PoC https://crums-io.github.io/timechain/overview.html
[^12]: Merkle tree implementation (used in skip ledger) https://crums-io.github.io/merkle-tree/

