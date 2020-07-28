# Sequential Consistency Checker

The couchbase.seqchecker module implements a sequential consistency checker for Jepsen histories. This document aims to explain the design and implementation of this checker.

## Background

Viotti and Vukolić provide a thorough overview of consistency semantics [1]. They introduce two relations that are useful in describing consistency models [1, pg. 5]:

* The visibility relation (vis) describes which operations are visible to each other. (a,b) ∈ vis implies that changes made by operation a are visible to operation b.
* The arbitration ordering (ar) describes the conflict resolution of operations. (a,b) ∈ ar implies that changes made by operation b override changes made by operation a.


## Sequential Consistency

Sequential Consistency is exactly equivalent to three properties: SingleOrder, PRAM, and RVal [1, pg. 11].

* SingleOrder requires that there is a single total ordering of all operations that simultaneously describes both visibility and arbitration order [1, pg. 8].

* PRAM (Pipeline Random Access Memory) requires that the order of operations for each process (the session-order) is a subset of the visibility order [1, pg. 11]. Note that since processes are logically single threaded, i.e. they only invoke a new operation after the previous operation has returned, the real-time ordering of operations by a single process is clearly defined. More practicaly, PRAM is equivalent to Monotonic Reads, Monotonic Writes and Read-your-writes as described by Jepsen's consistency model map [2].

* RVal requires each operation to return a value consistent with its datatype [1, pg. 5]. In effect, for our register workloads, this means that each read operation must return the value of the last (according to ar) visible (according to vis) write operation.


## Sequential Checker

The design and implementation of our sequential checker is simplified by enforcing limitations of which histories can be checked:
* Only read and write operations are allowed (no CAS)
* Each write invocation must have a unique value

Sequential consistency is then verified by attempting to construct a single-order that obeys PRAM and RVal constraints. A history is sequentially consistent if and only if such an ordering exists. Note that there may exist multiple such orderings for a given history; the particular constructed ordering is unimportant, only whether at least one exists.

To verify the existence of such an ordering the history is first partitioned into a queue of operations for each process, where the operations in those queues are ordered by session-order. A global single-order can then be constructed by considering which such queue yields the next operation. The next operation in the single-order must be the first operation from one of the queues, else the session-order would not be a subset of our single-order, violating PRAM.

If the first operation of any queue is a read operation whose value is equal to the value of the last operation added to the single-order, then that read can be serialized. All such reads must be serialized before any writes, since the unique writes requirement of our checker means that after any further write is serialized those reads will never be serializable again (without violating RVal). If there are multiple reads with the same value the ordering is irrelevant since reads are idempotent.

If no further serializable reads exist, then a write operation must be serialized. In this case the "correct" write needs to be picked. In particular, in order to avoid any "backtracking" in the checker logic, a write must be selected such that it is guaranteed if a single-order obeying PRAM and RVal exists, then the chosen write is the next op in at least one possible such ordering.

To guarantee this an operation write-x is selected where all read operations which return x would be serializable. More specifically, for each process that observes x, there must not exist any still unserialized operation with a value other than x before (according to session-order) the last read of that process that returns x.
If such an operation were to exist, then the unique writes requirement of the checker would make it impossible to serialize both that operation and the later read(s) returning value x.

There may still exist multiple potential writes that satisfy the above condition. The ordering of such writes is unimportant, since:
* A write is only considered serializable once all reads are serializable, so the reads of the chosen candidate must be serializable
* Read operations are unaffected by any write except for the last, so performing one write before another will not prevent the reads of other candidates being serializable later
* The current register value is irrelevant for write operations, so after serializing one write candidate the other candidates will still be serializable


## Further Implementation Details

Serialization of read operations is trivial as only the head of each queue needs to be checked. When serializing write operations, however, each process' op-queue need to be scanned to determine if any reads for the given value are unserializable. Checking all future reads could be prohibitively computationally expensive, so instead a map yielding the index of the last (in terms of real-time return) read for each value is created ahead of time. The op-queue then only needs to be scanned up to that index.

Failed operations are removed from the history prior to checking for sequential consistency. This is clearly valid since failed operations logically never happened. However, indeterminate writes (reads cannot be indeterminate) could cause significant complexity if both the scenario in which they did and did not logically happen needed to be considered. The unique writes requirement again allows the checker to be simplified. The last-read map can be reused to rewrite history such that no writes are indeterminate. If the value of an indeterminate write was observed by some read, then the write must have succeeded for the history to be sequentially consistent (else RVal would be violated). Thus any observed indeterminate writes can be safely rewritten as successful.

If the value of an indeterminate write is never observed, the history is rewritten such that the write failed. We note that if the write actually succeeded, the rewritten history will be "incorrect". This rewritten history can be shown to be sequentially consistent if and only if the real history is sequentially consistent.

In particular, the following two properties hold:
1. If the rewritten history is sequentially consistent, then the true history must also be sequentially consistent.
2. If the rewritten history is not sequentially consistent, then the true history cannot be sequentially consistent either.

For 1:  Since the rewritten history is sequentially consistent, there exists a single-order obeying PRAM and RVal characteristics for all operations expect some unobserved indeterminate writes. Consider a modified single-order where those writes are appended to the end of the rewritten history's single-order.
* Since Jepsen workers are logically single threaded, they do not invoke any further operations if their last operation was indeterminate. Thus there can only be one indeterminate write per process, and that write must be the last operation by that process. Therefore, the PRAM requirement that the session-order be a subset of the visibility relation cannot be violated by the modified single-order.
* Since the writes were inserted at the end of the single-order, they are not visible to any read operations. Thus the RVal condition cannot be violated.

Thus there exists a single-order obeying PRAM and RVal characteristics for the true history, so that history must also be sequentially consistent.


For 2: We show 2 by its contrapositive: if the true history is sequentially consistent, then the rewritten history is sequentially consistent.

Since the true history is sequentially consistent, there exists as single-order obeying PRAM and RVal characteristics for all operations. Consider now a rewritten history in which some unobserved writes are removed. For this history there exists a modified single-order, equivalent except for that removed operations are not present.
* The modified single-order is still a superset of the session-order, since in the modified history the removed operations never occurred and such are not part of the session-order either. Thus PRAM is not violated.
* Since the removed writes were unobserved, their value was, by definition, not the return value of any read. Thus removing the writes cannot cause the RVal condition to be violated.

Thus there exists a single-order obeying PRAM and RVal characteristics for the rewritten history, so that history must also be sequentially consistent.

The rewritten history thereby allows the sequential consistency of the true history to be checked without needing to consider indeterminate ops.

## Useful Resources

[1] P. Viotti and M. Vukolić, "Consistency in Non-Transaction Distributed Storage Systems". https://arxiv.org/pdf/1512.00168.pdf

[2] Jepsen Consistency Map: https://jepsen.io/consistency
