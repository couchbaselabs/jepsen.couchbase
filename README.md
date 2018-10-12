# jepsen.couchbase

Jepsen testing for Couchbase.

## Usage

### Requirements

A JVM and the [Leiningen](https://leiningen.org/) build tool need to be
installed, other dependencies will be auto-fetched on first run. Using vagrant
greatly simplifies the setup.

### Setup

Jepsen requires a cluster of nodes to run couchbase on, vagrant simplifies the
setup.
I assume this file is in `~/dev/jepsen.couchbase`, and vagrant files are in
`~/dev/vagrants`, replace paths as appropriate.

```
cd ~/dev/vagrants/5.5.1/centos7
export VAGRANT_NODES=5
vagrant up
```

Vagrant sets up an ssh key pair per node, but Jepsen doesn't support different
credentials per node. We don't care about the security of vagrants, so just set
a simple password for the vagrant user.
```
for i in $(seq 1 $VAGRANT_NODES); do vagrant ssh node$i -c "echo vagrant | sudo passwd --stdin vagrant"; done
```

We need to pass the node IPs to Jepsen, we store them in a file to make this
easier.
```
vagrant status | grep -o "\d\+.\d\+.\d\+.\d\+" > ~/dev/jepsen.couchbase/nodes
```

We run a basic test that should pass to ensure everything is working.
```
cd ~/dev/jepsen.couchbase
lein run test --nodes-file ./nodes --username vagrant --password vagrant --time-limit 30 --concurrency 15 --rate 0.25
```

### Demonstrate non-linearizability
1 replica and replicate-to = 0
```
lein run test --nodes-file ./nodes --username vagrant --password vagrant --time-limit 30 --concurrency 120 --replicas 1 --replicate-to 0 --rate 0.25 --nemesis partition-single
```
1 replica and replicate-to = 1
```
lein run test --nodes-file ./nodes --username vagrant --password vagrant --time-limit 30 --concurrency 60 --replicas 1 --replicate-to 1 --rate 0.25 --nemesis partition-single
```
2 replicas and replicate-to = 1
```
lein run test --nodes-file ./nodes --username vagrant --password vagrant --time-limit 30 --concurrency 240 --replicas 2 --replicate-to 1 --rate 0.25 --nemesis partition-then-failover
```

### Display results
Start a server on localhost:8080 to show the results with a pretty interface.
```
lein run serve
```
