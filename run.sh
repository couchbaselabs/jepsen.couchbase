#!/bin/bash

function print_usage() {
  echo "$(basename "$0") - Run Jepsen test suites for Couchbase
Usage: $(basename "$0") [options...]
Options:
  --suite=STRING                 Path to suite file
  --provisioner=STRING           Type of nodes to run the suite against: vagrant or docker
  --package=STRING               Path to the Couchbase Server package
" >&2
}

# read in parameters

for i in "$@"
do
case $i in
    --suite=*)
    SUITE="${i#*=}"
    ;;
    --provisioner=*)
    PROVISIONER="${i#*=}"
    ;;
    --package=*)
    PACKAGE="${i#*=}"
    ;;
    -h|--help)
    print_usage
    exit 0
    ;;
    *)
    print_usage
    exit 1
    ;;
esac
done

# parameter checks

if [ -z "$SUITE" ]; then
    echo "--suite not provided"
    exit 1
fi

if [ -z "$PACKAGE" ]; then
    echo "--package not provided"
    exit 1
fi

if [ "$PROVISIONER" != "vagrant" -a "$PROVISIONER" != "docker" ]; then
    echo "Provisioner must be either docker or vagrant, got $PROVISIONER"
fi


vagrantBaseCommand="lein trampoline run test"
vagrantParams="--nodes-file ./nodes --username vagrant --ssh-private-key ./resources/vagrantkey"
dockerBaseCommand="docker exec -it -w '/jepsen' jepsen-control lein trampoline run test"
dockerParams="--nodes-file ./nodes"
packageParam="--package $PACKAGE"
pass=0
fail=0
crash=0

while IFS='' read -r line || [[ -n "$line" ]]; do

    # ignore commented out tests in conf file
    if [[ $line == \#* ]]; then
        continue
    fi

    echo "Running test: $line"

    # parse conf file line for workload and parameters
    IFS=,
    ary=($line)
    testParams=""
    for key in "${!ary[@]}"
    do
        testParams="$testParams --${ary[$key]}"
    done

    # run test using vagrants or docker
    PREVIOUS_RUN=$(cd ./store/latest; pwd -P)

    if [ "$PROVISIONER" == "vagrant" ]; then
        command="$vagrantBaseCommand $testParams $vagrantParams $packageParam"
        echo ""
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
    fi
    if [ "$PROVISIONER" == "docker" ]; then
        docker exec -it jepsen-control bash -c "rm -rf /jepsen/store"
        command="$dockerBaseCommand $testParams $dockerParams $packageParam"
        echo ""
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
        docker cp jepsen-control:/jepsen/store .
    fi


    if [ $EXITCODE -ne 0 ]; then
        LAST_RUN=$(cd ./store/latest; pwd -P)
        if [ "$PREVIOUS_RUN" != "$LAST_RUN" ] && grep -q -e "^ :valid? false" store/latest/results.edn; then
            grep -m 1 -e ":workload.*" store/latest/jepsen.log | cut -d "\"" -f 2 | xargs printf "\n\nJepsen exited with failure for workload %s\n\n"
            echo "Test failed!\n\n"
            fail=$(($fail+1))
        else
            printf "Jepsen failed with unknown failure\n\n"
            echo "Test crashed!\n\n"
            crash=$(($crash+1))
        fi

    else
        printf "Jepsen tests passed\n\n"
        echo "Test passed!\n\n"
        pass=$(($pass+1))
    fi
done < "$SUITE"

total=$(($pass+$fail+$crash))
percent=`echo  "scale=2; $pass*100/$total" | bc`
echo "Jepsen tests complete!"
echo ""
echo "pass: $pass"
echo "fail: $fail"
echo "crash: $crash"
echo "$pass/$total = $percent%"
