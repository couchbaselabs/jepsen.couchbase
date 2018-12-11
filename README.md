# jepsen.couchbase

Jepsen testing for Couchbase.

## Usage

### Requirements

A JVM and the [Leiningen](https://leiningen.org/) build tool need to be
installed. Gnuplot is required to plot performance graphs. Other dependencies
will be auto-fetched on first run.

### Setup

Jepsen requires a cluster of nodes to run couchbase on, vagrant simplifies the
setup. I assume this file is in `~/dev/jepsen.couchbase`, and vagrant files are in
`~/dev/vagrants`, replace paths as appropriate. Using centos7 based vagrants
provides the most reliable setup. Assigning multiple cpus to each vm makes
replicating race conditions more likely.

```
cd ~/dev/vagrants/5.5.1/centos7
export VAGRANT_NODES=5
export VAGRANT_CPUS=2
vagrant up
```

Vagrant sets up an ssh key pair per node, but Jepsen doesn't support different
credentials per node. We don't care about the security of vagrants, so just set
a simple password for root ssh.
```
for i in $(seq 1 $VAGRANT_NODES); do vagrant ssh node$i -c "echo root | sudo passwd --root"; done
```

We need to pass the node IPs to Jepsen, we store them in a file to make this
easier.
```
vagrant status | grep -E -o "[0-9]+(\.[0-9]+){3}" > ~/dev/jepsen.couchbase/nodes
```

We run a basic test that should pass to ensure everything is working.
```
cd ~/dev/jepsen.couchbase
lein trampoline run test --nodes-file ./nodes --workload Register
```

### Demonstrate non-linearizability
1 replica and replicate-to = 0
```
lein trampoline run test --nodes-file ./nodes --workload Failover
```
1 replica and replicate-to = 1
```
lein trampoline run test --nodes-file ./nodes --workload MB30048
```
2 replicas and replicate-to = 1
```
lein trampoline run test --nodes-file ./nodes --workload MB28525
```

### Demonstrating fixed bugs
White-Rabbit
```
lein trampoline run test --nodes-file ./nodes --workload WhiteRabbit >> /dev/null
```
Lost DCP Mutations when cursor dropping
```
lein trampoline run test --nodes-file ./nodes --workload MB29369 >> /dev/null
```
Lost DCP deletions when cursor dropping
```
lein trampoline run test --nodes-file ./nodes --workload MB29480 >> /dev/null
```

### Displaying results
Start a server on localhost:8080 to show the results with a pretty interface.
```
lein run serve
```
