# jepsen.couchbase

Jepsen testing for Couchbase.

## Usage

### Requirements

A JVM and the [Leiningen](https://leiningen.org/) build tool need to be
installed. Gnuplot is required for the plot performance graphs option. Basic
utilities like bc, curl, grep are expected to be present. Clojure and Java
dependencies will be auto-fetched on first run. You need some nodes to run
the server on, scripts exist to start vagrant VMs or docker containers.


### Setup

Jepsen requires a cluster of nodes to run couchbase on. If you already have
nodes available you need to manually create ./nodes with the IP addresses.
Otherwise you can use the script run.sh to automatically start suitable vagrants
(or docker containers).


#### Getting started / Running a simple test

The provision script can be used to automatically start suitable VMs.
```
./provision.sh --type=vagrant --vm-os=ubuntu1604 --action=create --nodes=3
wget http://packages.couchbase.com/releases/6.0.0/couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb
```

You can then invoke leiningen to run a test

```
lein run test --nodes-file=./nodes --username=vagrant --ssh-private-key=./resources/vagrantkey --package=./couchbase-server-enterprise_6.0.0-ubuntu16.04_amd64.deb --workload=register
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
