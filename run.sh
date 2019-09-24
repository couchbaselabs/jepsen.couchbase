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
    --jenkins-run )
    JENKINS_RUN=true
    ;;
    --kv-cv-jenkins-run )
    KV_CV_RUN=true
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

if [ "$PROVISIONER" != "vagrant" -a "$PROVISIONER" != "docker" -a "$PROVISIONER" != "vmpool" -a "$PROVISIONER" != "cluster-run" ]; then
    echo "Provisioner must be either vagrant, docker, or cluster-run, but got $PROVISIONER"
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
unknown=0
memcached_failure=0
test_num=1
fail_array=()
crash_array=()
unknown_array=()
memcached_array=()

if [ "$JENKINS_RUN" ]; then
    rm -rf ./store
    mkdir ./store
    mkdir ./store/Couchbase
    mkdir ./store/Couchbase/pass
fi

suiteConfigName=$(basename $SUITE)
suiteName=${suiteConfigName%.*}

if [ -z $BUILD_TAG ] ; then
    testStoreName=$suiteName
else
    testStoreName=$BUILD_TAG
fi

if [[ $KV_CV_RUN ]]; then
    rm -f ./jepsen-output-*.log
fi

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
        command="$vagrantBaseCommand $vagrantParams $packageParam $testParams &> jepsen-output-$test_num.log"
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
    fi
    if [ "$PROVISIONER" == "docker" ]; then
        docker exec -it jepsen-control bash -c "rm -rf /jepsen/store"
        command="$dockerBaseCommand $dockerParams $packageParam $testParams &> jepsen-output-$test_num.log"
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
        docker cp jepsen-control:/jepsen/store .
    fi
    if [ "$PROVISIONER" == "vmpool" ]; then
        command="$vmpoolBaseCommand $vmpoolParams $packageParam $testParams &> jepsen-output-$test_num.log"
        echo "Test command: $command"
        echo ""
        eval $command
        EXITCODE=$?
    fi
    if [ "$PROVISIONER" == "cluster-run" ]; then
	command="lein trampoline run test --cluster-run $packageParam $testParams &> jepsen-output-$test_num.log"
	echo "Test command: $command"
	echo ""
	eval $command
	EXITCODE=$?
    fi

    LAST_RUN=$(cd ./store/latest; pwd -P)
    if [[ "$PREVIOUS_RUN" != "$LAST_RUN" ]]; then
        current_dir=$(pwd)
        cd ${LAST_RUN}
        find . -name "memcached.log*" -exec grep --with-filename --binary-files=text " ERROR \| CRITICAL " {} \; > error_msg_from_memcached.log
        if [[ $(wc -l < error_msg_from_memcached.log) -gt 0 ]]; then
            echo "Critical or Error log messages where found in memcached.log's"
            echo "******************* memcached errors ************************"
            cat error_msg_from_memcached.log
            echo "*************************************************************"
            memcached_failure=$(($memcached_failure+1))
            memcached_array+=("#$test_num :: $testParams")
        fi
        cd ${current_dir}
    fi

    if [ $EXITCODE -ne 0 ]; then
        cat "jepsen-output-$test_num.log"
        if [ "$PREVIOUS_RUN" != "$LAST_RUN" ] && grep -q -e "^ :valid? false" store/latest/results.edn; then
            grep -m 1 -e ":workload.*" store/latest/jepsen.log | cut -d "\"" -f 2 |
                xargs printf "\n\nJepsen exited with failure for workload %s\n\n"
            printf "Test failed!\n\n"
            fail=$(($fail+1))
            fail_array+=("#$test_num :: $testParams")
        else
            printf "Jepsen failed with unknown failure \n"
            printf "Test crashed! \n"
            crash=$(($crash+1))
            crash_array+=("#$test_num :: $testParams")
        fi
    else
        LAST_RUN=$(cd ./store/latest; pwd -P)
        if tail -n 1 $LAST_RUN/results.edn | grep -q ":valid? true" ; then
            pass=$(($pass+1))
            printf "Test passed!\n"
            if [ "$JENKINS_RUN" ]; then
                lastRunDir=$(ls ./store/Couchbase/ | grep -v 'latest' | grep -v 'pass' | tail -1)
                mv ./store/Couchbase/${lastRunDir} ./store/Couchbase/pass
                ln -s ./store/Couchbase/pass/${lastRunDir} ./store/latest
            fi
        elif tail -n 1 ${LAST_RUN}/results.edn | grep -q ":valid? :unknown" ; then
            unknown=$(($unknown+1))
            unknown_array+=("#$test_num :: $testParams")
            printf "Test history could not be validated\n"
        else
            printf "ERROR: Couldn't determine test status\n"
        fi
    fi

    total=$(($pass+$fail+$crash+$unknown))
    percent=`echo  "scale=2; $pass*100/$total" | bc`
    echo "###### Current Test Report #########"
    echo "pass: $pass"
    echo "unknown: $unknown"
    echo "fail: $fail"
    echo "crash: $crash"
    echo "memcached errors (not reported as total): $memcached_failure"
    echo "$pass/$total = $percent%"
    if [ "$unknown" -gt 0 ]; then
        echo "###### Unknown Tests ######"
        printf '%s\n' "${unknown_array[@]}"
    fi
    if [ "$fail" -gt 0 ];then
        echo "###### Failed Tests #########"
        printf '%s\n' "${fail_array[@]}"
    fi
    if [ "$crash" -gt 0 ];then
        echo "###### Crashed Tests ########"
        printf '%s\n' "${crash_array[@]}"
    fi
    if [ "$memcached_failure" -gt 0 ]; then
        echo "###### Memcached errors ######"
        printf '%s\n' "${memcached_array[@]}"
    fi
    echo "################################################################"

    if [ "$ISBUILD" = 1 ]; then
        packageParam="--install-path $PACKAGE";
    else
        packageParam="";
    fi

    test_num=$(($test_num+1))
done < "$SUITE"
echo "###### Finished Running Suite #########"

if [ $KV_CV_RUN ]; then
    # Rename this test run to the suite name we just ran
    if [ -d ./store/Couchbase-$testStoreName ]; then
        mv ./store/Couchbase/*  ./store/Couchbase-$testStoreName/
    else
        mv ./store/Couchbase/ ./store/Couchbase-$testStoreName
    fi
fi

total=$(($pass+$unknown+$fail+$crash))
percent=`echo  "scale=2; $pass*100/$total" | bc`
echo "Jepsen tests complete!" > ./test_report.txt
echo "###### Final Test Report #########" >> ./test_report.txt
echo "pass: $pass" >> ./test_report.txt
echo "unknown: $unknown" >> ./test_report.txt
echo "fail: $fail" >> ./test_report.txt
echo "crash: $crash" >> ./test_report.txt
echo "memcached errors (not reported as total): $memcached_failure" >> ./test_report.txt
echo "$pass/$total = $percent%" >> ./test_report.txt
if [ "$unknown" -gt 0 ]; then
    echo "###### Unknown Tests ########" >> ./test_report.txt
    printf '%s\n' "${unknown_array[@]}" >> ./test_report.txt
fi
if [ "$fail" -gt 0 ];then
    echo "###### Failed Tests #########" >> ./test_report.txt
    printf '%s\n' "${fail_array[@]}" >> ./test_report.txt
fi
if [ "$crash" -gt 0 ];then
    echo "###### Crashed Tests ########" >> ./test_report.txt
    printf '%s\n' "${crash_array[@]}" >> ./test_report.txt
fi
if [ "$memcached_failure" -gt 0 ]; then
    echo "###### Memcached errors ######" >> ./test_report.txt
    printf '%s\n' "${memcached_array[@]}" >> ./test_report.txt
fi
echo "################################################################" >> ./test_report.txt
cat ./test_report.txt
if [ "$JENKINS_RUN" ]; then
    mv ./test_report.txt ./store/Couchbase
    rm -rf ./store/Couchbase/latest
    rm -rf ./store/latest
    rm -rf ./store/current
fi

# These exit in the order their written. We need to ensue failures are always reported
# over a crash or unknown result.
if [ "$fail" -gt 0 ]; then
    exit 3
elif [ "$crash" -gt 0 ]; then
    exit 4
elif [ "$unknown" -gt 0 ]; then
    exit 2
elif [ "$memcached_failure" -gt 0 ]; then
    exit 3
else
    exit 0
fi
