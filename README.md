# jepsen.couchbase

Jepsen testing for Couchbase.

## Usage

### Requirements

A JVM and the [Leiningen](https://leiningen.org/) build tool need to be
installed. Gnuplot is required for the plot performance graphs option. Basic
utilities like bc, curl, grep are expected to be present. Clojure and Java
dependencies will be auto-fetched on first run. You need some Couchbase Server
nodes to run Couchbase Server. We provide the provision.sh script that starts
suitable nodes using vagrant VMs or docker containers. We also provide the
ability to run Jepsen using cluster-run type nodes from a local build. Note
that some workloads are incompatible with docker or cluster-run nodes.


### Getting started with cluster-run

Using cluster-run style nodes provides a simple way to start running tests. The
only required setup is to download and build the Couchbase Server source. *Note*
that Jepsen will automatically start the required cluster-run nodes. No other
cluster-run nodes should be running on the machine, and any leftover data from
previous cluster-run nodes will be deleted. Currently, due to an issue (JDCP-81)
with the Java DCP client used by Jepsen, set workloads crash if the server
reports version 0.0.0, as is the default for custom builds. This can be worked
around by setting a product version when building, for example with
`EXTRA_CMAKE_OPTIONS='-DPRODUCT_VERSION="9.9.9-9999"'`

```
lein run test --cluster-run --node-count 3 --workload register --package ~/dev/source/install
```

### Setup for vagrant / docker nodes

If not using cluster-run, Jepsen requires a cluster of nodes to run
Couchbase Server on. If you already have nodes available you need to
manually create a nodes file with the IP addresses. Otherwise you can
use the script provision.sh that automatically start suitable vagrants
(or docker containers) and create the corresponding file.


#### Getting started / Running a simple test

The provision script can be used to automatically start suitable VMs.
```
./provision.sh --type=vagrant --vm-os=ubuntu1604 --action=create --nodes=3
wget http://packages.couchbase.com/releases/6.0.0/couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```

You can then invoke leiningen to run a test

```
lein run test --nodes-file=./nodes --package=./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb --workload=register
```

After the test are completed the vagrants the VMs can be torn down with
```
./provision.sh --type=vagrant --action=destroy-all
```

#### Using 'suite' files

The script run.sh can be used to automatically run multiple tests from a config
file
```
./provision.sh --type=vagrant --vm-os=ubuntu1604 --action=create --nodes=3
./run.sh --provisioner=vagrant --package=./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb --suite=./suites/example.conf
```

The run.sh script can also be used with cluster-run nodes.

```
./run.sh --provisioner=cluster-run --package=~/dev/source/install --suite=./suites/example.conf --global=node-count:4
```

#### Using other nodes

To use other nodes provide the file ./nodes with one node IP per line. Test can
then be run with the following command (replacing user/pass with suitable ssh
credentials for the node).
```
./run.sh --provisioner=vmpool --package=./some-package.deb --suite=./some-suite.conf --global=username:user,password:pass
```

### Running against a build directory
If the host has a directory containing a build suitable for running the nodes,
you can supply the install directory to jepsen instead of a deb/rpm package
```
./run.sh ... --package ~/dev/source/install
```

### Displaying results
Start a server on `http://localhost:8080` to show the results with a simple interface.
```
lein trampoline run serve
```

## Development
### Checking your commit
Before push your code for review its important to check that the patch will pass our commit validation. 
To do this use:
```bash
chmod +x cv-checks.sh
./cv-checks.sh
```
This will run the same checks that are used on our Jenkins commit validation job. Its also important if adding or 
editing a workload/nemesis that they are tested, as the commit validator is unable to do this.