#!/bin/bash

# Copies the public key './resources/my.key.pub' to the given node's file of
# authorized ssh keys '~/.ssh/authorized_keys'. Assumes the expect tool and
# ssh-copy-id are installed by default.
# Note that by passing `-o StrictHostKeyChecking=no`, the ~/.ssh/known_host
# file gets populated by a fingerprint automatically.
copy_public_key_to_node() {
    if ! expect -v &> /dev/null
    then
        echo "Please install 'expect'."
        exit 1
    fi

    # Use expect to pass the username and password to ssh-copy-id and exit with
    # ssh-copy-id's exit code.
    expect -c "
    spawn ssh-copy-id -i ./resources/my.key.pub $2@$1 -o StrictHostKeyChecking=no
    expect password:
    send $3\n
    expect eof
    catch wait result
    exit [lindex \$result 3]
    " >/dev/null 2>&1

    exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "Error: ssh-copy-id exited with exit status: $exit_code."
        exit 1
    fi
}

# Calls 'copy_public_key_to_node' for all nodes.
copy_public_key_to_all_nodes() {
    while read node; do
        copy_public_key_to_node "$node" "$1" "$2"
    done <./nodes
}

print_usage() {
    echo "$(basename "$0") - Create and deploy ssh keys to nodes.
    Usage: $(basename "$0") [options...]
    Options:
      --action=STRING        Action to take:
                             *          create_keys - Creates and places key pair
                                                      in the ./resources folder.
                             *  cleanup_known_hosts - Removes entries in ~/.ssh/
                                                      known_hosts for all nodes
                                                      in ./nodes.
                             *  publish_public_keys - Publishes public keys for
                                                      all nodes in ./nodes.
      --username=STRING      The common ssh username for all of the nodes.
      --password=STRING      The common ssh password for all of the nodes.
    " >&2
}

# Creates a key-pair in the 'resources folder if one doesn't already exist.
# Creates './resources/my.key' and './resources/my.key.pub'
create_key_pair() {
    # If one of 'my.key' or 'my.key.pub' does not exist, (re)create them.
    if [ ! -f ./resources/my.key ] || [ ! -f ./resources/my.key.pub ]
    then
       # The 'yes' | Deals with the situation in which the user has accidently
       # deleted one of the key files by answering 'y' when ssh-keygen asks for
       # permission to override the existing keys.

       # The key file has to be of the PEM format for compability purposes
       # with Jepsen and is not password protected.
        yes | ssh-keygen -t rsa -N "" -f ./resources/my.key -m PEM
    else
        echo "Key pair already exists."
    fi
}

# Removes existing entries in the ~/.ssh/known_hosts for all nodes.
delete_all_nodes_from_known_hosts() {
    while read node; do
        ssh-keygen -R "$node"
    done <./nodes
}

for i in "$@"
do
case $i in
    -e=*|--action=*)
    ACTION="${i#*=}"
    shift
    ;;
    -s=*|--username=*)
    USERNAME="${i#*=}"
    shift
    ;;
    -l=*|--password=*)
    PASSWORD="${i#*=}"
    shift
    ;;
    -h|--help)
    print_usage
    ;;
    *)
    print_usage
    ;;
esac
done

case "$ACTION" in
    "create_keys")
        echo "Creating key-pair ./resources/my.key and ./resources/my.key.pub."
        create_key_pair
    ;;
    "cleanup_known_hosts")
        echo "Removing node entries from ~/.ssh/known_hosts."
        delete_all_nodes_from_known_hosts
    ;;
    "publish_public_key")
        if [[ -z "$USERNAME" ]] || [[ -z "$PASSWORD" ]] ; then
            echo "Please provide --username and --password."
            exit 1
        fi

        echo "Publishing public key ./resources/my.key.pub to all nodes."
        copy_public_key_to_all_nodes $USERNAME $PASSWORD
    ;;
    *)
        echo "Invalid --action: $ACTION"
        print_usage
        exit 1
    ;;

esac
