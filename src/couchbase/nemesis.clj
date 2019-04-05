(ns couchbase.nemesis
  (:require [clojure [set    :as set]
             [string :as string]]
            [clojure.tools.logging :refer :all]
            [couchbase [util      :as util]]
            [jepsen    [control   :as c]
             [generator :as gen]
             [nemesis   :as nemesis]
             [net :as net]]
            [cheshire.core :refer :all]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn device-mapper
  "Create a virtual block device using the linux kernel device mapper and mount
  a filesystem from that device as the couchbase server data directory. This
  allows for simulated disk issues. This nemesis will not work correctly with
  docker containers"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (let [path        (:install-path test)
            server-path (str path "/bin/couchbase-server")
            data-path   (str path "/var/lib/couchbase/data")]
        (c/with-test-nodes test
          (c/su (c/exec :dd "if=/dev/zero" "of=/tmp/cbdata.img" "bs=1M" "count=512")
                (c/exec :losetup "/dev/loop0" "/tmp/cbdata.img")
                (c/exec :dmsetup :create :cbdata :--table (c/lit "'0 1048576 linear /dev/loop0 0'"))
                (c/exec :mkfs.ext4 "/dev/mapper/cbdata")
                (c/exec server-path :-k)
                (c/exec :mv :-T data-path "/tmp/cbdata")
                (c/exec :mkdir data-path)
                (c/exec :mount :-o "noatime" "/dev/mapper/cbdata" data-path)
                (c/exec :chmod "a+rwx" data-path)
                (c/exec :mv :-t data-path (c/lit "/tmp/cbdata/*") (c/lit "/tmp/cbdata/.[!.]*")))
          (c/ssh* {:cmd (str "nohup " server-path " -- -noinput >> /dev/null 2>&1 &")})
          (util/wait-for-daemon)
          (util/wait-for-warmup))
        this))
    (invoke! [this test op]
      (case (:f op)
        :fail (let [fail (->> test :nodes shuffle (take (op :count)))]
                (c/on-many
                 fail
                 (c/su (c/exec :dmsetup :wipe_table :cbdata :--noflush :--nolockfs)
                       ;; Drop buffers. Since most of our tests use little data we can read
                       ;; everything from the filesystem level buffer despite the block device
                       ;; returning errors.
                       (c/exec :echo "3" :> "/proc/sys/vm/drop_caches")))
                (assoc op :value [:failed fail]))
        :slow (let [slow (->> test :nodes shuffle (take (op :count)))]
                (c/on-many
                 slow
                 ;; Load a new (inactive) table that delays all disk IO by 25ms.
                 (c/su (c/exec :dmsetup :load :cbdata :--table
                               (c/lit "'0 1048576 delay /dev/loop0 0 25 /dev/loop0 0 25'"))
                       (c/exec :dmsetup :resume :cbdata)))
                (assoc op :value [:slow slow]))
        :reset-all (do (c/on-many
                        (test :nodes)
                        (c/su (c/exec :dmsetup :load :cbdata :--table
                                      (c/lit "'0 1048576 linear /dev/loop0 0'"))
                              (c/exec :dmsetup :resume :cbdata)))
                       (assoc op :value :ok))))

    (teardown! [this test])))

(defn filter-nodes
  "This function will take in node-state atom and targeter-opts. Target conditions will be extracted from
  targeter-opts and used to filter nodes represented in node-states. Targeter conditions should be passed in as
  a map with keys :cluster :node and :network. The values for each key should be a vector of eligible state
  keywords. If a particular key is not present in the map, this function will consider all state keywords as
  eligible for that particular key.
  Example:
  {:condition {:cluster [:active :failed] :node [:running}} will return all nodes whose cluster state is either
  active or failed, node state is running and any network state."
  [node-states targeter-opts]
  (let [filter-conditions (:condition targeter-opts)
        cluster-condition (if (nil? (:cluster filter-conditions)) (set [:active :failed :inactive :ejected]) (set (:cluster filter-conditions)))
        network-condition (if (nil? (:network filter-conditions)) (set [:connected :partitioned]) (set (:network filter-conditions)))
        node-condition    (if (nil? (:node filter-conditions)) (set [:running :killed]) (set (:node filter-conditions)))
        server-group-condition (set (:server-group filter-conditions))
        cluster-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? cluster-condition (get-in v [:state :cluster]))] k))
        network-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? network-condition (get-in v [:state :network]))] k))
        node-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? node-condition (get-in v [:state :node]))] k))
        server-group-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? server-group-condition (get-in v [:state :server-group]))] k))
        matching-nodes (set/intersection (set (keys cluster-match-nodes)) (set (keys network-match-nodes)) (set (keys node-match-nodes)))
        sg-matching-nodes (if (nil? (:server-group filter-conditions)) matching-nodes (set/intersection matching-nodes (set (keys server-group-match-nodes))))]
    (vec sg-matching-nodes)))

(defn apply-targeter
  "This function takes in a list of nodes and a targeter-opts map that specifies how to select a subset of the nodes.
  The function will apply a function to the list of nodes based on a keyword (:type) in the target-opts map."
  [filtered-nodes targeter-opts]
  (let [targeter-type (:type targeter-opts)
        target-seq
        (case targeter-type
          :first (take 1 filtered-nodes)
          :random (take 1 (shuffle filtered-nodes))
          :random-subset (take (:count targeter-opts) (shuffle filtered-nodes))
          :all filtered-nodes)]
    (vec target-seq)))

(defn get-targets
  "This function takes in an atom representing node states and a targeter-opts map. The function will first
  apply a filter to the list of nodes based on node state, then it will target a subset of the filtered nodes
  and return the select nodes as a vector"
  [node-states targeter-opts]
  (let [filtered-nodes (filter-nodes node-states targeter-opts)
        target-nodes (apply-targeter filtered-nodes targeter-opts)]
    target-nodes))

(defn create-grudge
  "This function will create a partition grudge to be based to Jepsen. Partitions look like [(n1) (n2 n3) (n4)].
  Each sequence represents partitions such that only nodes within the same sequence can communicate."
  [partition-type target-nodes all-nodes]
  (case partition-type
    :isolate-completely
    (let [complement-nodes (seq (set/difference (set all-nodes) (set target-nodes)))
          partitions (conj (partition 1 target-nodes) complement-nodes)
          grudge (nemesis/complete-grudge partitions)]
      grudge)))

(defn update-node-state
  "This function takes in a atom of node states, a target node, a map of state keys with a single value to update
  the current node state with."
  [node-states target new-states]
  (let [node-state (get-in @node-states [target :state])
        updated-state (merge node-state new-states)]
    (swap! node-states assoc-in [target :state] updated-state)))

(defn set-node-server-group-state
  [node-states]
  (let [server-group-info (util/rest-call (first (keys @node-states)) "/pools/default/serverGroups" nil)
        server-group-json (parse-string server-group-info true)
        server-groups (:groups server-group-json)]
    (doseq [group server-groups]
      (doseq [node (:nodes group)]
        (let [group-name (:name group)
              otpNode (:otpNode node)
              node-name (string/replace otpNode #"ns_1@" "")]
          (update-node-state node-states node-name {:server-group group-name}))))))

(defn couchbase
  "The Couchbase nemesis represents operations that can be taken against a Couchbase cluster. Nodes are
  represented as a map atom where the keys are node ips and the values are state maps. State maps store node
  state in vectors of keywords. Each invoke can select a subset of nodes to act upon by filtering nodes
  based on state.After selecting a set of nodes, the nemesis will take the requested action and update node state
  accordingly."
  []
  (let [nodes (atom [])
        node-states (atom {})]
    (reify nemesis/Nemesis
      (setup! [this test]
        (net/heal! (:net test) test)
        (reset! nodes (:nodes test))
        (reset! node-states (reduce #(assoc %1 %2 {:state {:cluster :active :network :connected :node :running}}) {} (:nodes test)))
        (if (:server-groups-enabled test)
          (set-node-server-group-state node-states))
        this)

      (invoke! [this test op]
        (info "op: " (str op))
        (let [f-opts                (:f-opts op)
              targeter-opts         (:targeter-opts op)
              target-nodes          (if (nil? targeter-opts) @nodes (get-targets @node-states targeter-opts))
              active-nodes          (filter-nodes @node-states {:condition {:cluster [:active]}})
              failed-nodes          (filter-nodes @node-states {:condition {:cluster [:failed]}})
              ejected-nodes         (filter-nodes @node-states {:condition {:cluster [:ejected]}})
              inactive-nodes        (filter-nodes @node-states {:condition {:cluster [:inactive]}})
              connected-nodes       (filter-nodes @node-states {:condition {:network [:connected]}})
              partitioned-nodes     (filter-nodes @node-states {:condition {:network [:partitioned]}})
              running-nodes         (filter-nodes @node-states {:condition {:node [:running]}})
              killed-nodes          (filter-nodes @node-states {:condition {:node [:killed]}})
              cluster-nodes         (filter-nodes @node-states {:condition {:cluster [:active :inactive :failed]}})
              failover-nodes        (filter-nodes @node-states {:condition {:cluster [:active :inactive]}})
              healthy-cluster-nodes (filter-nodes @node-states {:condition {:cluster [:active]
                                                                            :network [:connected]
                                                                            :node [:running]}})]
          (case (:f op)
            :failover
            ; failover will not work if there is an :inactive :killed node in the cluster
            (let [failover-type (:failover-type f-opts)
                  endpoint (case failover-type
                             :hard "/controller/failOver"
                             :graceful "/controller/startGracefulFailover")]
              (assert (< (count target-nodes)
                         (count failover-nodes))
                      "failover count must be less than total active and inactive nodes")
              (doseq [target target-nodes]
                (let [call-node (case (:call-node f-opts)
                                  :target-node target
                                  nil (first healthy-cluster-nodes))]
                  (info "target node: " (str target))
                  (info "call node: " (str call-node))
                  (util/rest-call call-node endpoint (str "otpNode=ns_1@" target))
                  (if (= failover-type :graceful) (util/wait-for-rebalance-complete call-node))
                  (update-node-state node-states target {:cluster :failed})))
              (info "cluster state: " @node-states)
              (assoc op :value :failover-complete))

            :recover
            ; recovery will not work if there is an :inactive :killed node in the cluster
            (let [recovery-type (:recovery-type f-opts)
                  eject-nodes (vec (set/difference (set failed-nodes) (set target-nodes)))
                  call-node (first healthy-cluster-nodes)]
              (assert (<= (count target-nodes) (count failed-nodes)) "recovery count must be less or equal to the total failed node count")
              (doseq [target target-nodes]
                (let [params (str (format  "otpNode=%s&recoveryType=%s" (str "ns_1@" target) (str (name recovery-type))))]
                  (info "setting recovery type to " (str (name recovery-type)) "for node " target)
                  (util/rest-call call-node "/controller/setRecoveryType" params)))
              (info "cluster nodes: " cluster-nodes)
              (util/rebalance cluster-nodes)
              (doseq [target target-nodes]
                (update-node-state node-states target {:cluster :active}))
              (doseq [eject-node eject-nodes]
                (update-node-state node-states eject-node {:cluster :ejected}))
              (info "cluster state: " @node-states)
              (assoc op :value :recovery-complete))

            :partition-network
            (let [partition-type (:partition-type f-opts)
                  grudge (create-grudge partition-type target-nodes @nodes)]
              (info "Partition grudge: " (str grudge))
              (net/drop-all! test grudge)
              (doseq [target target-nodes]
                (update-node-state node-states target {:network :partitioned :cluster :inactive}))
              (info "cluster state: " @node-states)
              (assoc op :value [:isolated grudge]))

            :heal-network
            (do
              (net/heal! (:net test) test)
              (doseq [target @nodes]
                (let [target-cluster-state (get-in @node-states [target :state :cluster])
                      target-node-state (get-in @node-states [target :state :node])
                      target-network-state (get-in @node-states [target :state :network])]
                  (if (and (= target-cluster-state :inactive) (= target-node-state :running) (= target-network-state :partitioned))
                    (update-node-state node-states target {:cluster :active :network :connected})
                    (update-node-state node-states target {:network :connected}))))
              (info "cluster state: " @node-states)
              (assoc op :value :network-healed))

            :wait-for-autofailover
            (let [target (first target-nodes)
                  initial-count (util/get-autofailover-info target :count)
                  final-count (+ initial-count 1)
                  autofailover-count (atom initial-count)
                  node-info-before (util/get-node-info target)]
              (util/wait-for #(util/get-autofailover-info target :count) final-count 120)
              (let [node-info-after (util/get-node-info target)]
                (doseq [node-info node-info-before]
                  (let [node-key (key node-info)
                        state-before (get-in node-info-before [node-key :clusterMembership])
                        state-after (get-in node-info-after [node-key :clusterMembership])
                        active-before (= state-before "active")
                        failed-after (= state-after "inactiveFailed")]
                    (if (and active-before failed-after)
                      (do (update-node-state node-states node-key {:cluster :failed}))))))
              (info "cluster state: " @node-states)
              (assoc op :value :autofailover-complete))

            :rebalance-out
            ; rebalance will not work if there is an :inactive :killed node in the cluster
            ; This function will attempt to rebalance out target nodes. It will also add any currently failed nodes
            ; to the set of nodes to be removed as these nodes would be removed even if they were omitted from
            ; the util/rebalance call. Issues may arise if util/add-nodes or
            ; util/rebalance fails. No error handling is implemented in this function but Jepsen
            ; will catch and exceptions and continue generating ops. Special care should be taken
            ; when using this function in a scenario where nodes are partitioned
            (let [nodes-to-eject (vec (set/union (set failed-nodes) (set target-nodes)))]
              (util/rebalance (set cluster-nodes) (set nodes-to-eject))
              (doseq [eject-node nodes-to-eject]
                (update-node-state node-states eject-node {:cluster :ejected})
                (update-node-state node-states eject-node {:server-group nil}))
              (info "cluster state: " @node-states)
              (assoc op :value (str "Removed: " nodes-to-eject)))

            :rebalance-in
            ; rebalance will not work if there is an :inactive :killed node in the cluster
            ; This function will attempt to rebalance in the target nodes. It will grab any currently failed
            ; nodes and set them for removal during rebalance as this would happen even if the failed nodes
            ; were omitted from util/rebalance. Issues may arise if util/add-nodes or
            ; util/rebalance fails. No error handling is implemented in this function but Jepsen
            ; will catch and exceptions and continue generating ops. Special care should be taken
            ; when using this function in a scenario where nodes are partitioned
            (do
              (c/on
                (first healthy-cluster-nodes)
                (util/add-nodes (set target-nodes) (get-in f-opts [:add-opts] nil)))
              (util/rebalance (set/union (set cluster-nodes) (set target-nodes)) (set failed-nodes))
              (doseq [target-node target-nodes]
                (info "updating target node state in rebalance-in")
                (update-node-state node-states target-node {:cluster :active})
                (update-node-state node-states target-node {:server-group (util/get-node-group target-node)})
                )
              (doseq [failed-node failed-nodes]
                (info "updating failed node state in rebalance-in")
                (update-node-state node-states failed-node {:cluster :ejected})
                (update-node-state node-states failed-node {:server-group nil}))
              (info "cluster state: " @node-states)
              (assoc op :value (str "Added: " target-nodes " Removed: " failed-nodes)))

            :swap-rebalance
            ; rebalance will not work if there is an :inactive :killed node in the cluster
            ; This function will grab all nodes in the cluster, sets target nodes to be
            ; added to the cluster by issuing rest call to the first non-partitioned node
            ; in the cluster, and sets an equal number of nodes in the cluster to be removed
            ; which will accomplish a swap rebalance. Issues may arise if util/add-nodes or
            ; util/rebalance fails. No error handling is implemented in this function but Jepsen
            ; will catch and exceptions and continue generating ops. Special care should be taken
            ; when using this function in a scenario where nodes are partitioned
            (let [nodes-to-remove (vec (take (count target-nodes) (shuffle (vec cluster-nodes))))
                  static-nodes (vec (set/difference (set healthy-cluster-nodes) (set nodes-to-remove)))]
              (c/on (first static-nodes) (util/add-nodes (set target-nodes)))
              (util/rebalance (set/union (set cluster-nodes) (set target-nodes)) (set nodes-to-remove))
              (doseq [add-node target-nodes]
                (update-node-state node-states add-node {:cluster :active}))
              (doseq [remove-node nodes-to-remove]
                (update-node-state node-states remove-node {:cluster :ejected}))
              (info "cluster state: " @node-states)
              (assoc op :value (str "Added: " target-nodes " Removed: " (vec nodes-to-remove))))

            :fail-rebalance
            (let [kill-target (if (nil? (:kill-target f-opts))
                                (throw (RuntimeException. (str "kill-target not found in f-opts")))
                                (:kill-target f-opts))
                  rebalance  (future (util/rebalance cluster-nodes target-nodes))]
              ;; Sleep between 2 and 4 seconds to allow the rebalance to start
              (Thread/sleep (+ (* (rand) 2000) 2000))
              ;; Kill memcached on a different random collection of nodes
              (case kill-target
                :same-nodes
                (c/on-many target-nodes (c/su (c/exec :pkill :-9 :memcached))))
              ;; Wait for rebalance to quit, swallowing rebalance failure
              (try @rebalance (catch Exception e (warn "Rebalance failed")))
              (assoc op :value (str "Rebalance failed for nodes: " (str target-nodes))))

            :kill-process
            (let [process (:process f-opts)]
              (case process
                :memcached
                (do
                  (c/on-many target-nodes (c/su (c/exec :pkill :-9 :memcached)))
                  (info "cluster state: " @node-states)
                  (assoc op :value [:killed :memcached target-nodes]))
                :ns-server
                (do
                  (c/on-many target-nodes (c/su (c/exec :bash :-c "kill -9 $(pgrep beam.smp | tail -n +2)")))
                  (info "cluster state: " @node-states)
                  (assoc op :value [:killed :ns_server target-nodes]))
                :babysitter
                (do
                  (c/on-many target-nodes
                             (c/su (c/exec :bash :-c "kill -9 $(pgrep beam.smp | head -n 1)")))
                  (doseq [killed-node target-nodes]
                    (if (contains? (set cluster-nodes) killed-node)
                      ;node will become inactive after being killed only if it is in the cluster
                      (update-node-state node-states killed-node {:cluster :inactive :node :killed})
                      (update-node-state node-states killed-node {:node :killed})))
                  (info "cluster state: " @node-states)
                  (assoc op :value [:killed :babysitter target-nodes]))))

            :start-process
            (let [process (:process f-opts)]
              (case process
                :couchbase-server
                (let [path (:install-path test)]
                  (c/on-many
                   target-nodes
                   (c/ssh* {:cmd (str "nohup " path "/bin/couchbase-server -- -noinput >> /dev/null 2>&1 &")})
                   (util/wait-for-daemon))
                  (doseq [started-node target-nodes]
                    (if (contains? (set inactive-nodes) started-node)
                      ; inactive nodes will become active after starting couchbase-server
                      ; the time to become active again is around 3 seconds, we should wait
                      (do
                        ; should wait here until node status is healthy
                        (util/wait-for #(get-in (util/get-node-info (first healthy-cluster-nodes)) [started-node :status]) "healthy" 30)
                        (update-node-state node-states started-node {:cluster :active :node :running}))
                      ; failed over nodes will only be recoverable if couchbase-server is running
                      (update-node-state node-states started-node {:node :running})))
                  (info "cluster state: " @node-states)
                  (assoc op :value [:started :couchbase-server target-nodes]))))

            :slow-dcp-client
            (let [dcpclient (:dcpclient (:client test))]
              (reset! (:slow dcpclient) true)
              (info "cluster state: " @node-states)
              (assoc op :type :info))

            :reset-dcp-client
            (let [dcpclient (:dcpclient (:client test))]
              (reset! (:slow dcpclient) false)
              (info "cluster state: " @node-states)
              (assoc op :type :info))

            :trigger-compaction
            (do
              (util/rest-call (rand-nth (test :nodes)) "/pools/default/buckets/default/controller/compactBucket" "")
              (info "cluster state: " @node-states)
              op)

            :noop
            (do
              (info "cluster state: " @node-states)
              (assoc op :value "noop")))))

      (teardown! [this test]))))

