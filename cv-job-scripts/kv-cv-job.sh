#!/usr/bin/env bash

EXIT_CODE=0
GIT_ROOT_TEST="$(git rev-parse --show-toplevel)"
PROVISIONER=""
PACKAGE_NAME=""

function print_usage() {
  echo "$(basename "$0") - Running Jepsen CV Job in the Jenkins environment.
Usage: $(basename "$0") [options...]
Options:
  --use-vms=BOOL                 If the job should provision vms on the Jepsen master build machine
  --branch=STRING                Branch name of the build e.g. mad-hatter or cheshire-cat (in lowercase)
  --platform-build-string        String of the platform suffix needed to download the relevant build, e.g. 'ubuntu18.04_amd64.deb' or 'centos7.x86_64.rpm'.
  --test-suit=STRING             Path from the root of this repo to the test conf file that should but used
  --web-server=STRING            Url of where a user can find the test run logs.
" >&2
}

# parse input parameters
for i in "$@"
do
case ${i} in
    --use-vms=*)
    USE_VMS="${i#*=}"
    ;;
    --branch=*)
    BRANCH="${i#*=}"
    ;;
    --platform-build-string=*)
    PLATFORM_BINARY_STR="${i#*=}"
    ;;
    --test-suit=*)
    TEST_SUIT="${i#*=}"
    ;;
    --web-server=*)
    WEB_URL="${i#*=}"
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

if [[ -z "$USE_VMS" ]]; then
    print_usage
    exit 1
fi

if [[ -z "$BRANCH" ]]; then
    print_usage
    exit 1
fi

if [[ -z "$TEST_SUIT" ]]; then
    print_usage
    exit 1
fi

if [[ -z "$BUILD_TAG" ]]; then
    echo "Env var BUILD_TAG isn't provided"
    exit 1
fi

if [[ -z "$WORKSPACE" ]]; then
    echo "Env var WORKSPACE isn't provided"
    exit 1
fi

if [[ -z "$NETWORK_INTERFACE" ]]; then
    NETWORK_INTERFACE="eth0"
    echo "Setting NETWORK_INTERFACE to ${NETWORK_INTERFACE}"
fi

function provision_vms() {
    PROVISIONER="vagrant"
    ${GIT_ROOT_TEST}/provision.sh --type=vagrant --action=destroy-all
    rm -f ${GIT_ROOT_TEST}/nodes
    ${GIT_ROOT_TEST}/provision.sh --type=vagrant --action=create --nodes=4 --handle-numa-cv

    if [[ $? -ne 0 ]]; then
        echo "FAILED TO START VAGRANTS, ABORTING"
        exit 2
    fi
}

function setup_node_file() {
    PROVISIONER="vmpool"
    if [[ -f ${GIT_ROOT_TEST}/nodes ]]; then
        echo "Using Node IPs"
        cat ${GIT_ROOT_TEST}/nodes
    else
        echo "Node nodes file"
        exit 1
    fi
}

function destory_vms() {
    if [[ $(ls -1 ${GIT_ROOT_TEST}/resources/dumps | wc -l) -gt 0 ]]; then
        echo "Found crash dumps in dump directory!!!"
        echo "$(ls -l ${GIT_ROOT_TEST}/resources/dumps)"
        zip -r $(BUILD_TAG)-core-dump.zip ${GIT_ROOT_TEST}/resources/dumps
    fi

    echo "Tearing down VMs"
    ${GIT_ROOT_TEST}/provision.sh --type=vagrant --action=destroy-all
}

function download_build() {
    PACKAGE_NAME="couchbase-server-enterprise_$BRANCH-${PLATFORM_BINARY_STR}"
    if [[ -f ${GIT_ROOT_TEST}/${PACKAGE_NAME} ]]; then
        rm -rf ${GIT_ROOT_TEST}/${PACKAGE_NAME}
    fi

    rm -vf ${GIT_ROOT_TEST}/*.deb
    rm -vf ${GIT_ROOT_TEST}/*.rpm
    wget -4 -o download.log "http://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/$BRANCH/latest/$PACKAGE_NAME"
    BUILD_VERSION=$(dpkg-deb -f ${PACKAGE_NAME} Version)
    echo "Couchbase Server: $BUILD_VERSION"
}

function run_test_suite() {
    ;; GLOBAL_RUN_ARGS="enable-tcp-capture,hashdump,enable-memcached-debug-log-level"
    GLOBAL_RUN_ARGS="enable-tcp-capture,hashdump"
    if [[ ${USE_VMS} != "true" ]]; then
        GLOBAL_RUN_ARGS="${GLOBAL_RUN_ARGS},net-interface=${NETWORK_INTERFACE}"
    fi
    echo "################################################################"
    echo "Running test suit: ${TEST_SUIT}"
    cat ${TEST_SUIT}
    echo "################################################################"
    ./run.sh --suite=${TEST_SUIT} --provisioner=${PROVISIONER} --package=${PACKAGE_NAME} --kv-cv-jenkins-run --setup-keys --purge-logs-on-success --global="${GLOBAL_RUN_ARGS}"
    EXIT_CODE=$?
}

function check_memcached_logs() {
    foundErrorLogsMsg=false
    LogFileLocation="${GIT_ROOT_TEST}/store/Couchbase-${BUILD_TAG}"
    cd ${LogFileLocation}
    find . -name "memcached.log*" -exec grep --with-filename --binary-files=text " ERROR \| CRITICAL " {} \; > error_msg_from_memcached.log

    if [[ $(wc -l < error_msg_from_memcached.log) -gt 0 ]]; then
        foundErrorLogsMsg=true
        echo "Error log messages where found in memached.log's"
        cat error_msg_from_memcached.log
        if [[ "$EXIT_CODE" -eq "0" ]]; then
            EXIT_CODE=5
        fi
        if [[ "$EXIT_CODE" -eq "4" ]]; then
            EXIT_CODE=6
        fi
    fi
}

function process_exit_code() {
    cd ${WORKSPACE}
    echo "EXIT CODE IS $EXIT_CODE"
    # Always exit with exit code 2 (unstable) unless a definite linearizability error occurred
    case ${EXIT_CODE} in
         0)
         echo "LINEARIZABILITY PASS"
         exit 0
         ;;
         1)
         echo "INVALID TEST CONFIGURATION"
         exit 2
         ;;
         2)
         echo "INDETERMINATE RESULT"
         exit 2
         ;;
         3)
         echo "LINEARIZABILITY ERROR"
         exit 3
         ;;
         4)
         echo "JEPSEN CRASHED"
         exit 2
         ;;
         5)
         echo "LINEARIZABILITY PASS BUT ERRORS FOUND IN MEMCACHED LOGS"
         exit 3
         ;;
         6)
         echo "JEPSEN CRASHED AND ERRORS FOUND IN MEMCACHED LOGS"
         exit 3
         ;;
         *)
         echo "UNKNOWN ERROR"
         exit 2
         ;;
    esac
}

echo "------    Testing Jepsen changes against latest build    ------"
# clean up any old data files so there's enough disk space for this Job
${GIT_ROOT_TEST}/cv-job-scripts/purge-old-test-data.sh
download_build;
if [[ ${USE_VMS} == "true" ]]; then
    provision_vms;
else
    setup_node_file;
fi
run_test_suite
if [[ ${USE_VMS} == "true" ]]; then
    destory_vms;
fi
check_memcached_logs;
process_exit_code;
if [[ -z "$WEB_URL" ]]; then
    echo "You can view the test logs here:${WEB_URL}"
fi



