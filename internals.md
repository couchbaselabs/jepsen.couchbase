# Internal structure

## Components

A workload specifies all the test parameters: the client, nemesis, op generator and couchbase server setup parameters. This allows a tuned workload to provide consistent results to replicate a specific issue.

Upon startup a pool of couchbase java sdk clients are created. Each jepsen client threads is assigned a single sdk client in a round-robin fashion, which it used for the duration of that thread's lifetime. This ensures that multiple threads serving the same key use different sdk clients.

The predefined workloads combine a client, nemesis and op generator designed to trigger a specific issue. The register class of workloads are suitable for strict checking of linearizability on a per key basis. Since linearizability checking is NP-Hard, the register workloads are not suitable when a large opcount is required to trigger an issue, in such cases the set workloads are more suitable. The set workload is optimized for high opcounts, it rapidly inserts keys into a bucket, then checks for their presence in a dcp stream. This allow for issues where data is lost in ep-engine as well as issues isolated to a single dcp stream to be detected.

## Extending

To define a new workload create a new function in couchbase.workload with a name of the form `new-workload` taking a map of the cli options as it's single argument and returning a map of test parameters. The workload can then be run using the cli argument `--workload new`. When there a several similar variations on a workload, the scenario parameter can be used to add multiple "scenarios" to one workload function.

New nemeses actions can be added either by create a new nemesis or extending an existing one. The create a new nemesis create function in couchbase.nemesis that reifies jepsen.nemesis/Nemesis, either directly or through one of the helper functions in jepsen.nemesis. This can then be added to a workload. Use jepsen.nemesis/compose if you need actions from multiple nemeses in a single workload. Alternatively additional actions can be added to an existing nemesis.
