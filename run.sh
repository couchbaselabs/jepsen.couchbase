#!/bin/bash
echo "Running jepsen tests..."
lein trampoline run "$@" --nodes-file ./nodes --username vagrant --ssh-private-key ./resources/vagrantkey
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "Jepsen tests complete"
    if grep -q -e "^ :valid? false" store/latest/results.edn; then
	    grep -m 1 -e ":workload.*" store/latest/jepsen.log | cut -d "\"" -f 2 | xargs printf "\n\nJepsen exited with failure for workload %s\n\n"
    else
	    printf "Jepsen exited with unknown failure\n\n"
    fi
    exit 1
else
    printf "Jepsen tests passed\n\n"
    exit 0
fi
