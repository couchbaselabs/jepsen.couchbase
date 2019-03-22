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
    --global=*)
    GLOBAL="${i#*=}"
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

if [ "$PROVISIONER" != "vagrant" -a "$PROVISIONER" != "docker" -a "$PROVISIONER" != "vmpool" ]; then
    echo "Provisioner must be either docker or vagrant, got $PROVISIONER"
fi

case $PACKAGE in
    *.rpm)
	ISBUILD=0 ;;
    *.deb)
	ISBUILD=0 ;;
    *)
	ISBUILD=1 ;;
esac

vagrantBaseCommand="lein trampoline run test"
vagrantParams="--nodes-file ./nodes --username vagrant --ssh-private-key ./resources/vagrantkey"
dockerBaseCommand="docker exec -it -w '/jepsen' jepsen-control lein trampoline run test"
dockerParams="--nodes-file ./nodes"
vmpoolBaseCommand="lein trampoline run test"
vmpoolParams="--nodes-file ./nodes --username root --password couchbase"
packageParam="--package $PACKAGE"
pass=0
fail=0
crash=0
test_num=1
fail_array=()
crash_array=()

while IFS='' read -r line || [[ -n "$line" ]]; do

    # ignore commented out tests in conf file
    if [[ $line == \#* ]]; then
        continue
    fi

    echo "################################################################"
    echo "Running test: #$test_num"
    echo "Test params: $line"

    # parse conf file line for workload and parameters
    IFS=,
    ary=($line)
    testParams=""
    for key in "${!ary[@]}"
    do
        testParams="$testParams --${ary[$key]}"
    done

    IFS=,
    ary=($GLOBAL)
    for key in "${!ary[@]}"
    do
        global_param=${ary[$key]}
        global_param=${global_param//:/=}
        testParams="$testParams --${global_param}"
    done

    # run test using vagrants or docker
    PREVIOUS_RUN=$(cd ./store/latest; pwd -P)

    if [ "$PROVISIONER" == "vagrant" ]; then
        command="$vagrantBaseCommand $vagrantParams $packageParam $testParams"
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
    fi
    if [ "$PROVISIONER" == "docker" ]; then
        docker exec -it jepsen-control bash -c "rm -rf /jepsen/store"
        command="$dockerBaseCommand $dockerParams $packageParam $testParams"
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
        docker cp jepsen-control:/jepsen/store .
    fi
    if [ "$PROVISIONER" == "vmpool" ]; then
        command="$vmpoolBaseCommand $vmpoolParams $packageParam $testParams"
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
    fi


    if [ $EXITCODE -ne 0 ]; then
        LAST_RUN=$(cd ./store/latest; pwd -P)
        if [ "$PREVIOUS_RUN" != "$LAST_RUN" ] && grep -q -e "^ :valid? false" store/latest/results.edn; then
            grep -m 1 -e ":workload.*" store/latest/jepsen.log | cut -d "\"" -f 2 | xargs printf "\n\nJepsen exited with failure for workload %s\n\n"
            printf "Test failed!\n\n"
            fail=$(($fail+1))
            fail_array+=($test_num)
        else
            printf "Jepsen failed with unknown failure \n"
            printf "Test crashed! \n"
            crash=$(($crash+1))
            crash_array+=("#$test_num")
        fi

    else
        printf "Jepsen tests passed\n"
        printf "Test passed!\n"
        pass=$(($pass+1))
    fi

    total=$(($pass+$fail+$crash))
    percent=`echo  "scale=2; $pass*100/$total" | bc`
    echo "###### Current Test Report #########"
    echo "pass: $pass"
    echo "fail: $fail"
    echo "crash: $crash"
    echo "$pass/$total = $percent%"
    if [ "$fail" -gt 0 ];then
        echo "###### Failed Tests #########"
        printf '%s\n' "${fail_array[@]}"
    fi
    if [ "$crash" -gt 0 ];then
        echo "###### Crashed Tests ########"
        printf '%s\n' "${crash_array[@]}"
    fi
    echo "############################"
    
    if [ "$ISBUILD" = 1 ]; then
	packageParam="--install-path $PACKAGE";
    else
	packageParam="";
    fi

    test_num=$(($test_num+1))
done < "$SUITE"
echo "################################################################"

total=$(($pass+$fail+$crash))
percent=`echo  "scale=2; $pass*100/$total" | bc`
echo "Jepsen tests complete!"
echo "###### Final Test Report #########"
echo "pass: $pass"
echo "fail: $fail"
echo "crash: $crash"
echo "$pass/$total = $percent%"
if [ "$fail" -gt 0 ];then
    echo "###### Failed Tests #########"
    printf '%s\n' "${fail_array[@]}"
fi
if [ "$crash" -gt 0 ];then
    echo "###### Crashed Tests ########"
    printf '%s\n' "${crash_array[@]}"
fi
echo "############################"

if [ "$fail" -gt 0 ] || [ "$crash" -gt 0 ];then
    exit 1
else
    exit 0
fi
