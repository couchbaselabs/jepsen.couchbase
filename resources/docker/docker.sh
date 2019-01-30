#!/bin/sh
set -e # exit on an error

ERROR(){
    /bin/echo -e "\e[101m\e[97m[ERROR]\e[49m\e[39m $@"
}

WARNING(){
    /bin/echo -e "\e[101m\e[97m[WARNING]\e[49m\e[39m $@"
}

INFO(){
    /bin/echo -e "\e[104m\e[97m[INFO]\e[49m\e[39m $@"
}

exists() {
    type $1 > /dev/null 2>&1
}

NODES=${NODES:-3}

for i in $@; do
    case $i in
        --help)
            HELP=1
            ;;
        --start)
            INFO "Start docker-compose daemon"
            START=1
            ;;
	--stop)
	    INFO "Bringing down containers"
	    STOP=1
	    ;;
	--resume)
	    INFO "Resume stopped containers"
	    RESUME=1
	    ;;
	--destroy)
	    INFO "Removing containers and volumes"
	    DESTROY=1
	    ;;
	--nodes=*)
	    NODES="${i#*=}"
	    ;;
        *)
            ERROR "unknown option $1"
            exit 1
            ;;
    esac
    shift
done

if [ "$HELP" ]; then
    echo "usage: $0 [OPTION]"
    echo "  --help                   Display this message"
    echo "  --nodes=N                Number of containers to start, only used with --start"
    echo "  --start                  Start containers"
    echo "  --stop                   Stop containers"
    echo "  --resume                 Restart stopped containers"
    echo "  --destroy                Remove the containers and their volumes"
    echo
    exit 0
fi

if [ "$START" ]; then
    # Dockerfile does not allow `ADD ..`. So we need to copy that.
    INFO "Copying .. to control/jepsen"
    (
	rm -rf ./control/jepsen
	mkdir ./control/jepsen
	(cd ../..; tar --exclude=./resources/docker --exclude=./.git --exclude=./store -cf - .)  | tar Cxf ./control/jepsen -
    )

    exists docker || { ERROR "Please install docker (https://docs.docker.com/engine/installation/)"; exit 1; }
    exists docker-compose || { ERROR "Please install docker-compose (https://docs.docker.com/compose/install/)"; exit 1; }

    # Remove any existing containers
    if [ -f docker-compose.yml ]; then
	docker-compose -f docker-compose.yml stop
        docker-compose -f docker-compose.yml rm -f -v
    fi

    # Generate new docker-compose file with correct number of nuodes
    printf "version: '2'\n"                       >  ./docker-compose.yml
    printf "services:\n"                          >> ./docker-compose.yml
    printf "  control:\n"                         >> ./docker-compose.yml
    printf "    container_name: jepsen-control\n" >> ./docker-compose.yml
    printf "    hostname: control\n"              >> ./docker-compose.yml
    printf "    build: ./control\n"               >> ./docker-compose.yml
    printf "    cap_add:\n"                       >> ./docker-compose.yml
    printf "      - NET_ADMIN\n"                  >> ./docker-compose.yml
    printf "      - NET_RAW\n"                    >> ./docker-compose.yml
    printf "    links:\n"                         >> ./docker-compose.yml

    for i in $(seq 1 $NODES); do
	printf "      - n%d\n" $i >> ./docker-compose.yml
    done

    printf "  node:\n"                         >> ./docker-compose.yml
    printf "    container_name: jepsen-node\n" >> ./docker-compose.yml
    printf "    build: ./node\n"               >> ./docker-compose.yml
    printf "    cap_add:\n"                       >> ./docker-compose.yml
    printf "      - NET_ADMIN\n"                  >> ./docker-compose.yml
    printf "      - NET_RAW\n"                    >> ./docker-compose.yml

    for i in $(seq 1 $NODES); do
	printf "  n%d:\n"                         $i >> ./docker-compose.yml
	printf "    extends: node\n"                 >> ./docker-compose.yml
	printf "    container_name: jepsen-n%d\n" $i >> ./docker-compose.yml
	printf "    hostname: n%d\n"              $i >> ./docker-compose.yml
    done

    INFO "Running \`docker-compose build\`"
    docker-compose build

    INFO "Running \`docker-compose up\`"
    docker-compose -f docker-compose.yml up -d --force-recreate

    docker exec -it jepsen-control bash -c "rm -f /jepsen/nodes"
    for i in $(seq 1 $NODES); do
	docker exec -it jepsen-control bash -c "getent hosts n$i | awk '{ print \$1 }' >> /jepsen/nodes";
    done
    INFO "All containers started"
fi

if [ "$STOP" ]; then
    docker-compose -f docker-compose.yml stop
fi

if [ "$RESUME" ]; then
    docker-compose -f docker-compose.yml up -d --no-recreate
fi

if [ "$DESTROY" ]; then
    INFO "Destroying containers and volumes"
    docker-compose -f docker-compose.yml stop
    docker-compose -f docker-compose.yml rm -f -v
fi
