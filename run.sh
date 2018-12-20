#!/bin/bash
export VAGRANT_CWD=./resources
vagrant up
vagrant ssh node1 -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >  ./nodes
vagrant ssh node2 -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >> ./nodes
vagrant ssh node3 -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >> ./nodes
echo "Running jepsen tests..."
lein trampoline run "$@" --nodes-file ./nodes --username vagrant --ssh-private-key ./resources/vagrantkey
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "Jepsen tests complete, halting vagrants"
    if grep -q -e "^ :valid? false" store/latest/results.edn; then
	grep -m 1 -e ":workload.*" store/latest/jepsen.log | cut -d "\"" -f 2 | xargs printf "\n\nJepsen exited with failure for workload %s\n\n"
    else
	printf "Jepsen exited with unknown failure\n\n"
    fi
    vagrant halt
    exit 1
else
    printf "Jepsen tests passed\n\n"
    vagrant halt
    exit 0
fi
