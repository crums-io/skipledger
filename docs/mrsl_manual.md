mrsl Manual
========

This is a short manual for the *mrsl* tool.  
Version 0.0.3

## Contents

- [Overview](#overview)
    - [Use Model](#use-model)
- [Commands](#commands)
    - [sum](#sum)
    - [state](#state)
    - [list](#list)
    - [history](#history)
    - [entry](#entry)
    - [merge](#merge)

## Overview

Morsel files are tamper proof binary containers of row data from a historical append-only ledger. Since they're in binary format, we need this program to validate, read and manipulate them.

Here's what you may find in a morsel file (`.mrsl`):

1. *opaque row hashes only*. These are called *state-morsel*s. Such a morsel only reveals how many rows are in the ledger it represents as
well as how the hash of the last row is related to that of the first in the ledger. This information is compact no matter how many rows the ledger has. Think of state morsels as rich fingerprints: as a ledger evolves its new fingerprint can be
validated against its older ones.

2. *source rows*. A morsel may include any reasonably small subset of source rows from a ledger. (Morsels are
designed to be, well.. morsels: they're supposed to fit in memory.) Any column value in these source rows may
have been redacted by its substitution with a hash. (The hashing procedure resists both
rainbow attacks and frequency analysis.)

3. *witness records*. A morsel may also contain one or more tamper proof records (called *crumtrail*s) indicating the time
the hash of a particular row in the ledger (identified by its row number) was witnessed by the `crums.io` service. Since
the hash of every row in the ledger [also] depends on the hash of every row before it, a crumtrail for a given row number
establishes the minimum age of that row and *every row before that row number*.

### Use Model

Morsels originate from ledger owners, but morph once in the wild, as they're lumped together, or sliced and diced with
redactions (coming features), etc. Exactly how these tamper proof files are to be disseminated, is left open. Let's illustrate the "trust model" by a hypothetical example:

Say, you have a morsel. It's a tamper proof window into a historical ledger. Yes, you can verify
that it hasn't been tampered with (by the fact mrsl loads it without a fuss).
But which ledger is it from? Is this morsel from a made-up ledger? Or is it from the "Chinook Music Corp Recievables" ledger
that you were promised?

As it turns out, "Chinook Music Corp" periodically publishes the latest state of this "Receivables" ledger as a state morsel
on their website (the rich-fingerprint thing). This is good: you can verify your morsel indeed came from the same ledger posted
on the Chinook Music Corp website. (See [merge](#merge).)




## Commands

The following is meant to complement the program's `-help` option. None of the commands require
a network connection. Argument order does not matter.

### sum

This prints a summary of the morsel's contents.

Here's a summary of a *state* morsel:

>
    $ mrsl chinook/chinook-state-900.mrsl sum
    
    <chinook-state-900.mrsl>
    
    Rows:
     count:          13                                    
     # range:        lo: 1                                 hi: 900                  
     with sources:   0                                     
    
    <900-d82b4c90067230270eed86d0eabbad3692431e16464756c194f69b51170b3665>

There's no ceremony: it says there are zero rows with source attachments. The highest row number in the morsel
is 900: so this morsel captures the state of the ledger when it had 900 rows. The hash of row [900], or equivalently the
hash of the ledger when it had exactly 900 rows, is displayed on the last line.

And here's a summary of a morsel with more stuff in it:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl sum
    
    <chinook-260-259-258-257-.mrsl>
    
    Rows:
     count:          27                                    
     # range:        lo: 1                                 hi: 800                  
     with sources:   8                                     
    
    History:
     created before: Sat Oct 23 22:32:56 MDT 2021          (row 300)                
    
    <800-15796914851707b5499604d65cf46189809cb9812155a822d2facba9d1ef264b>

Inspecting the last line, this morsel captures the state of the ledger when it had 800 rows. It contains 8 rows with
sources which were all created before Sat Oct 23 22:33 (because their hashes were witnessed then).

### state

This just prints the last line output by the *sum* command without the brackets.

### list

This lists all the rows in the morsel by row number. Witness dates and row source data, if present, are displayed.

Example:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl list
    1                                         
    2                                         
    4                                         
    8                                         
    16                                        
    32                                        
    64                                        
    128                                       
    253   S   253 47 1518 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC .. 
    254   S   254 47 1527 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC .. 
    255   S   255 47 1536 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC .. 
    256   S W Sat Oct 23 22:32:56 MDT 2021    256 47 1..                            
    257   S   257 47 1554 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC .. 
    258   S   258 47 1563 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC .. 
    259   S   259 47 1572 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC .. 
    260   S   260 47 1581 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC .. 
    264                                       
    272                                       
    288                                       
    296                                       
    300    W  Sat Oct 23 22:32:56 MDT 2021    
    304                                       
    320                                       
    384                                       
    512                                       
    768                                       
    800                                       
    
    27 rows; 2 crumtrails; 8 source-rows.

Rows with source-attachements are marked with `S`; if their hash has been witnessed, then they're marked with a `W` and
the witness date immediately follows.

What's in the empty rows? Nothing but hash data linking each successive row to the previous one.

### history

This lists the *crumtrail*s (witness records) in the morsel evidencing when the hash of rows were witnessed.
Because the hash of each row depends on the hash of every row before it, witnessing the hash of a row at a given [row] number
also implies witnessing every row before that row number.

Example:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl history
    
    
    2 rows witnessed in <chinook-260-259-258-257-.mrsl>:
    
    [row #] [date; ref hash; ref URL]                                               
    ------- -------------------------                                               
    
    256     Sat Oct 23 22:32:56 MDT 2021                                            
            8a1457684b393a346b461ac674eb4e7a617d5aaa6033d703af9f79173092f38f        
            https://crums.io/api/list_roots?utc=1635050186576&count=-4              
    300     Sat Oct 23 22:32:56 MDT 2021                                            
            8a1457684b393a346b461ac674eb4e7a617d5aaa6033d703af9f79173092f38f        
            https://crums.io/api/list_roots?utc=1635050186576&count=-4              
    
The left column indicates the row number witnessed; the right column is a synopsis of the crumtrail:

1. The date witnessed
2. The root hash of the crumtrail
3. A reference URL for the crumtrail's hash

(The crumtrail for row [256] in the example above is redundant, by the way. It's there because rows at such
numbers--multiples of higher powers of 2, are special and useful to keep around).


### entry

This provides a more detailed listing of source rows (if any).

Example:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl entry 253-260
    
    253         source-row as text:             
    --------------------------------------------------------------------------------
    253 47 1518 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    254         source-row as text:             
    --------------------------------------------------------------------------------
    254 47 1527 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    255         source-row as text:             
    --------------------------------------------------------------------------------
    255 47 1536 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    256         source-row as text:             
    --------------------------------------------------------------------------------
    256 47 1545 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    257         source-row as text:             
    --------------------------------------------------------------------------------
    257 47 1554 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    258         source-row as text:             
    --------------------------------------------------------------------------------
    258 47 1563 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    259         source-row as text:             
    --------------------------------------------------------------------------------
    259 47 1572 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    260         source-row as text:             
    --------------------------------------------------------------------------------
    260 47 1581 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC Canada V6C 1G8 13.86
    --------------------------------------------------------------------------------
    300         created before Sat Oct 23 22:32:56 MDT 2021  (crumtrail) 
     ref hash:  8a1457684b393a346b461ac674eb4e7a617d5aaa6033d703af9f79173092f38f 
     ref URL:   https://crums.io/api/list_roots?utc=1635050186576&count=-4 
    
    8 entries, 2 crumtrails

Presently, column values are simply displayed as text. (JSON output with richer type information
is slated for version 0.0.4.) Redacted column values are marked with an ` [X] `.

### merge

The *merge* command takes 2 or more morsel files and merges them into one. But you can't merge just any 2 morsels. In order to be mergeable,
any 2 morsels must satisfy the following criteria:

1. The hash of every row (identified by their row number) in both morsels must agree.
2. The 2 morsels must contain enough information to connect the hash of highest numbered row with that every source row in both morsels.

Put another way, we can only ever merge morsels from the same ledger.

Example:

>
    $ mrsl merge chinook-260-259-258-257-.mrsl chinook-state-900.mrsl save=chinook-900-merged.mrsl
    
    Loading 2 morsels for merge..
     chinook-260-259-258-257-.mrsl
     chinook-state-900.mrsl
    
     authority: chinook-state-900.mrsl
    
    init 14 objects (hi row 900)
     chinook-260-259-258-257-.mrsl ... 25 objects added
    
    39 objects merged to chinook-900-merged.mrsl

When 2 or more morsels are merged together, the morsel containing [the hash of] the highest row number is the
*authority*. (If there's a tie, one is chosen at random.) Above, `chinook-state-900.mrsl` was the authority since
it had the highest row number.

The newly written morsel `chinook-900-merged.mrsl` now contains all the source rows in both original morsels, and runs
to row [900]. In this example, one of the morsels was a state morsel (with no source rows). So this merge operation
simply updated the old morsel `chinook-260-259-258-257-.mrsl` to reflect the hash of the latest rows in the ledger.
But it also serves another purpose: it validates that the information in `chinook-260-259-258-257-.mrsl` indeed comes
from the same ledger that produced `chinook-state-900.mrsl`.














