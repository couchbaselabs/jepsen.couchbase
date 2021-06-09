#!/bin/bash

function print_usage() {
  echo "$(basename "$0") - Create, stop, and destroy nodes using vagrant or docker
Usage: $(basename "$0") [options...]
Options:
  --type=STRING                 Type of nodes to provision:vagrant or docker
  --action=STRING               Action to take, i.e create, halt, destroy, etc
  --nodes=INT                   Number of nodes to take action against. Default=3
  --handle-numa-cv              States if we need to pin vcpus against real cpu cores to mitigate against NUMA.
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
    --handle-numa-cv)
    HANDLE_NUMA_CV=true
    ;;
    --vm-os=*)
    VM_OS="${i#*=}"
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

if [[ -z "$TYPE" ]]; then
    echo "--type not provided"
    exit 1
fi

if [[ -z "$ACTION" ]]; then
    echo "--action not provided"
    exit 1
fi

case "$TYPE" in
    "vagrant")
        export VAGRANT_CWD=./resources
        case "$ACTION" in
            "create")
                if [[ NODES -eq 0 ]]; then
                    echo "--nodes must be greater than 0"
                fi

                if [[ -z "$VM_OS" ]]; then
                    echo "--vm-os not provided, defaulting to ubuntu2004"
                    VM_OS="ubuntu2004"
                else
                    case "$VM_OS" in
                    "ubuntu1604")
                    VM_OS="ubuntu1604"
                    ;;
                    "ubuntu2004")
                    VM_OS="ubuntu2004"
                    ;;
                    "centos7")
                    VM_OS="centos7"
                    ;;
                    *)
                    echo "--vm-os choice not recognized, exiting"
                    exit 1
                    ;;
                    esac

                fi
                # create will ensure that the number of nodes requested is the number of nodes present
                # and will extract ips into nodes file
                currentNodes=0
                if [[ -d "./resources/.vagrant/machines/" ]]; then
                  currentNodes=$(ls -1U ./resources/.vagrant/machines/ | wc -l)
                fi

                if [[ currentNodes -eq 0 ]]; then
                    cp ./resources/vagrant/${VM_OS} ./resources/Vagrantfile
                fi


                if [[ NODES -ge currentNodes ]]; then
                    NODES=${NODES} vagrant up
                else
                    for (( i=currentNodes; i>NODES; i-- ))
                    do
                        NODES=${i} vagrant destroy node${i} --force && rm -rf ./resources/.vagrant/machines/node${i}
                    done
		            NODES=${NODES} vagrant up
                fi
                for (( i=1; i<=$(ls -1U ./resources/.vagrant/machines/ | wc -l); i++ ))
                do
                    if [[ "$VM_OS" = "ubuntu2004" ]]; then
                        if [[ i -eq 1 ]]; then
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet [0-9]+(\.[0-9]+){3}" | cut -d " " -f 2- >  ./nodes
                        else
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet [0-9]+(\.[0-9]+){3}" | cut -d " " -f 2- >>  ./nodes
                        fi
                    fi
                    if [[ "$VM_OS" = "ubuntu1604" ]]; then
                        if [[ i -eq 1 ]]; then
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >  ./nodes
                        else
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >>  ./nodes
                        fi
                    fi
                    if [[ "$VM_OS" = "centos7" ]]; then
                        if [[ i -eq 1 ]]; then
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet [0-9]+(\.[0-9]+){3}" | cut -d " " -f 2- >  ./nodes
                        else
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet [0-9]+(\.[0-9]+){3}" | cut -d " " -f 2- >>  ./nodes
                        fi
                    fi
                done

                ./ssh_keys.sh --action=create_keys
                ./ssh_keys.sh --action=publish_public_key --username=root --password=root

                # We now need to check if we need to pin vcpus
                if [[ ${HANDLE_NUMA_CV} ]]; then
                        #NUMA node layout for our KV-Engine Jepsen host
                        #NUMA node0 CPU(s):     0,2,4,6,8,10,12,14
                        #NUMA node1 CPU(s):     1,3,5,7,9,11,13,15

                        VM_LIST=$(virsh list | awk '/resources/ {print $2}')
                        VM_NUM=$(echo  ${VM_LIST} | wc -w)
                        PIN_CPU=0
                        for VM in ${VM_LIST}; do
                            if [[ ${VM_NUM} -gt 13 ]]; then
                                echo "Could not provision Nodes correctly";
                                exit 1
                            fi
                             virsh vcpupin ${VM} --vcpu 0 ${PIN_CPU}
                             virsh vcpupin ${VM} --vcpu 1 $((PIN_CPU + 2))

                             virsh numatune ${VM} --nodeset $(( PIN_CPU & 1))
                             PIN_CPU=$((PIN_CPU + 1))
                        done
                    fi
                ;;
            "halt-all")
                NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant halt
                ;;
            "resume-all")
                # resume-all will start all stopped vagrants in ./resources/.vagrant/machines and
                # extract the ips into the nodes file
                NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant up

                is_ubuntu1604=$(grep "ubuntu1604" ./resources/Vagrantfile)
                is_centos7=$(grep "centos7" ./resources/Vagrantfile)

                for (( i=1; i<=$(ls -1U ./resources/.vagrant/machines/ | wc -l); i++ ))
                do

                    if [[ "$is_ubuntu1604" ]]; then
                        if [[ i -eq 1 ]]; then
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >  ./nodes
                        else
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet addr:[0-9]+(\.[0-9]+){3}" | cut -d ":" -f 2- >>  ./nodes
                        fi
                    fi

                    if [[ "$is_centos7" ]]; then
                        if [[ i -eq 1 ]]; then
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet [0-9]+(\.[0-9]+){3}" | cut -d " " -f 2- >  ./nodes
                        else
                            NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant ssh node${i} -c "ifconfig eth1" | grep -o -E "inet [0-9]+(\.[0-9]+){3}" | cut -d " " -f 2- >>  ./nodes
                        fi
                    fi

                done
                ;;
            "destroy-all")
                NODES=$(ls -1U ./resources/.vagrant/machines/ | wc -l) vagrant destroy --force
                rm -rf ./resources/.vagrant
                rm -f ./resources/Vagrantfile
                ;;
        esac
        ;;
    "docker")
        case "$ACTION" in
	    "create")
		(cd ./resources/docker/ && sh docker.sh --start --nodes=${NODES})
		;;
	    "halt-all")
		(cd ./resources/docker/ && sh docker.sh --stop)
		;;
	    "resume-all")
		(cd ./resources/docker/ && sh docker.sh --resume)
		;;
	    "destroy-all")
		(cd ./resources/docker/ && sh docker.sh --destroy)
		;;
	esac
        ;;
esac
