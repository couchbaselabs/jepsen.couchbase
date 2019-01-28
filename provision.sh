#!/bin/bash

function print_usage() {
  echo "$(basename "$0") - Create, stop, and destroy nodes using vagrant or docker
Usage: $(basename "$0") [options...]
Options:
  --type=STRING                 Type of nodes to provision:vagrant or docker
  --action=STRING               Action to take, i.e create, halt, destroy, etc
  --nodes=INT                   Number of nodes to take action against. Default=3
" >&2
}

NODES=${NODES:-3}

# parse input parameters
for i in "$@"
do
case $i in
    --type=*)
    TYPE="${i#*=}"
    ;;
    --action=*)
    ACTION="${i#*=}"
    ;;
    --nodes=*)
    NODES="${i#*=}"
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

if [ -z "$TYPE" ]; then
    echo "--type not provided"
    exit 1
fi

if [ -z "$ACTION" ]; then
    echo "--action not provided"
    exit 1
fi

case "$TYPE" in
    "vagrant")
        export VAGRANT_CWD=./resources
        case "$ACTION" in
            "create")
                # create will ensure that the number of nodes requested is the number of nodes present
                # and will extract ips into nodes file
                currentNodes=0
                if [ -d "./resources/.vagrant/machines/" ]; then
                  currentNodes=$(ls -1U ./resources/.vagrant/machines/ | wc -l)
                fi
                if [[ NODES -ge currentNodes ]]; then
                    NODES=$NODES vagrant up
                else
                    for (( i=currentNodes; i>NODES; i-- ))
                    do
                        NODES=${i} vagrant destroy node${i} --force && rm -rf ./resources/.vagrant/machines/node${i}
                    done
                fi
                for (( i=1; i<=$(ls -1U ./resources/.vagrant/machines/ | wc -l); i++ ))
                do
                    if [[ i -eq 1 ]]; then
                        NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >  ./nodes
                    else
                        NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >>  ./nodes
                    fi
                done
                ;;
            "halt-all")
                NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant halt
                ;;
            "resume-all")
                # resume-all will start all stopped vagrants in ./resources/.vagrant/machines and
                # extract the ips into the nodes file
                NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant up
                for (( i=1; i<=$(ls -1U ./resources/.vagrant/machines/ | wc -l); i++ ))
                do
                    if [[ i -eq 1 ]]; then
                        NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >  ./nodes
                    else
                        NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >>  ./nodes
                    fi
                done
                ;;
            "destroy-all")
                NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant destroy --force && rm -rf ./resources/.vagrant
                ;;
        esac
        ;;
    "docker")

        ;;
esac