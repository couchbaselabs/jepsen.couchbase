# Internal structure

## Components

A workload specifies all the test parameters: the client, nemesis, op generator and couchbase server setup parameters. This allows a tuned workload to provide consistent results to replicate a specific issue

The client is split into two parts, the "jepsen client" and the "cbclient". The jepsen client receives ops from jepsen, converts them into a couchbase style operation and passed them on to the cbclient. The cbclient wrapper allow Jepsen to connect to the cluster using on of the following methods:
 - A single connection with the java sdk
 - A pool of multiple connections where each operations is dispatched to a random connection
 - A batching client that queues multiple operations before dispatching them as a batch operation on a single connection
 - A pool of batching clients

The cbclient is shared between all the jepsen clients, this allows jepsen to simultaneously schedule ops from hundreds or thousands of threads without overloading the server with connections. The batch and pool cbclients enable a higher op throughput which is required to expose some race conditions.
With the pool cbclient there is, however, no correlation between jepsen clients and server connections: the same jepsen client will send sequential ops using different connections. When testing for linearizability this is not an issue, as ops being visible to all clients in real time is a requirement of the consistency model, but it may be unsuitable for checking other consistency models.

The predefined workloads combine a client, nemesis and op generator designed to trigger a specific issue. The register class of workloads are suitable for strict checking of linearizability on a per key basis. Since linearizability checking is NP-Hard, the register workloads are not suitable when a large opcount is required to trigger an issue, in such cases the set workloads are more suitable. The set workload is optimized for high opcounts, it rapidly inserts keys into a bucket, then checks for their presence in a dcp stream. This allow for issues where data is lost in ep-engine as well as issues isolated to a single dcp stream to be detected.

## Extending

To define a new workload create a new function in couchbase.workload with a name of the form `WorkloadName-workload` taking a map of the cli options as it's single argument and returning a map of test parameters. The workload can then be run using the cli argument `--workload WorkloadName`.

To define a new nemesis create a new function in couchbase.nemesis that reifies jepsen.nemesis/Nemesis, either directly or through one of the helper functions in jepsen.nemesis. It can then be added to a workload in couchbase.workload




