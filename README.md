# jepsen.couchbase

Jepsen testing for Couchbase.

## Usage

### Requirements

A JVM and the [Leiningen](https://leiningen.org/) build tool need to be
installed. Gnuplot is required for the plot performance graphs option.
Other dependencies will be auto-fetched on first run. You need either a list
of IP addresses of nodes ready to ssh into, or have vagrant installed to
provision vms.

### Setup

Jepsen requires a cluster of nodes to run couchbase on. If you already have
nodes available you can invoke jepsen directly. Otherwise you can use the
script run.sh to automatically start suitable vagrants.

### Running a single test

The helper script run.sh can automatically start ubuntu 16.04 vagrants, you need
to supply a .deb couchbase package to install.
```
wget http://packages.couchbase.com/releases/6.0.0/couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
./run.sh test --workload Register --package ./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```

To use other nodes invoke lein directly, providing a file with one node IP per line
and ssh credentials (`--username` and either `--ssh-private-key` or `--password`).
The supplied user must have sudo access.
```
lein trampoline run test --workload Register --nodes-file some-file.txt --username node-username --password abc123 --package ./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```

The workload argument can specify any single workload to be run, see workload.clj
for currently defined workloads.

### Running multiple tests

With the multitest subcommand we can sequentially run multiple tests, aborting if
any of the tests fail. To run all the workloads are expected to pass on a recent
couchbase version we can run
```
./run.sh multitest --workload Register,Set,WhiteRabbit,MB29369,MB29480 --package ./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```

### Demonstrating non-linearizability
The following workloads demonstrate non-linearizability caused by node failovers
due to limitations of the current observe based durability.

1 replica and replicate-to = 0
```
./run.sh test --workload Failover --package ./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```
1 replica and replicate-to = 1
```
./run.sh test --workload MB30048 --package ./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```
2 replicas and replicate-to = 1
```
./run.sh test --workload MB28525 --package ./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```

### Demonstrating fixed bugs
Specifying a package for an older couchbase release allows us to demonstrate fixed bugs.

White-Rabbit
```
./run.sh test --workload WhiteRabbit --package ./couchbase-server-enterprise_5.0.1-ubuntu16.04_amd64.deb
```
Lost DCP Mutations when cursor dropping
```
./run.sh test --workload MB29369 --package ./couchbase-server-enterprise_5.0.1-ubuntu16.04_amd64.deb
```
Lost DCP deletions when cursor dropping
```
./run.sh test --workload MB29480 --package ./couchbase-server-enterprise_5.0.1-ubuntu16.04_amd64.deb
```

### Running against a build
If the host has a directory containing a build suitable for running the nodes,
you can supply the install directory to jepsen instead of a deb/rpm package
```
./run.sh test --workload Register --package ~/dev/source/install
```

### Displaying results
Start a server on `http://localhost:8080` to show the results with a simple interface.
```
lein trampoline run serve
```
