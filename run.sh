#!/bin/bash
echo "Running jepsen tests..."
case $1 in
    --vagrant)
        VAGRANT=1
	;;
    --docker)
	DOCKER=1
	;;
    *)
	echo "First argument must be --vagrant or --docker, got $1"
	exit 2
	;;
esac

PREVIOUS_RUN=$(cd ./store/latest; pwd -P)

if [ "$VAGRANT" ]; then
    lein trampoline run "${@:2}" --nodes-file ./nodes --username vagrant --ssh-private-key ./resources/vagrantkey ;
    EXITCODE=$?
fi

if [ "$DOCKER" ]; then
    docker exec -it jepsen-control bash -c "rm -rf /jepsen/store"
    docker exec -it -w "/jepsen" jepsen-control lein trampoline run test "${@:2}" --nodes-file ./nodes
    EXITCODE=$?
    docker cp jepsen-control:/jepsen/store .
fi

if [ $EXITCODE -ne 0 ]; then
    LAST_RUN=$(cd ./store/latest; pwd -P)
    if [ "$PREVIOUS_RUN" != "$LAST_RUN" ] && \
	   grep -q -e "^ :valid? false" store/latest/results.edn; then
	grep -m 1 -e ":workload.*" store/latest/jepsen.log | cut -d "\"" -f 2 | xargs printf "\n\nJepsen exited with failure for workload %s\n\n"
    else
	printf "Jepsen exited with unknown failure\n\n"
    fi
    exit 1
else
    printf "Jepsen tests passed\n\n"
    exit 0
fi
