(ns couchbase.nemesis
  (:require [clojure
             [set :as set]
             [string :as string]]
            [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase [util :as util]]
            [dom-top.core :refer [with-retry]]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]
             [net :as net]]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn basic-nodes-targeter
  "A basic targeter that selects :target-count (default 1) random nodes"
  [test op]
  (->> (:nodes test)
       (shuffle)
       (take (:target-count op 1))))

(defn failover
  "Using the rest-api trigger a failover of the target nodes"
  [test op]
  (assert (= (:f op) :failover))
  (let [fail-type (:failover-type op)
        target-nodes ((:targeter op) test op)
        endpoint (case fail-type
                   :hard  "/controller/failOver"
                   :graceful "/controller/startGracefulFailover")]
    (doseq [target target-nodes]
      (info "Failing over node" target)
      (util/rest-call target endpoint {:otpNode (util/get-node-name target)})
      (if (= fail-type :graceful) (util/wait-for-rebalance-complete target)))
    (assoc op :value target-nodes)))

(defn recover
  "Attempt to detect and recover all failed-over nodes in the cluster. Note that
  if some failure condition is still applied to the nodes, this will likely fail."
  [test op]
  (assert (= (:f op) :recover))
  (with-retry [retry-count 10]
    (let [status-maps (util/get-node-info-map test)

          ;; Assert that all nodes see each other as healthy, if not we can't
          ;; recover. Node may be marked as unhealthy if ns_server has not yet
          ;; detected that nodes are back up, so we want to retry.
          _ (doseq [[_ status-map] status-maps]
              (if (some #(not= "healthy" (:status %)) (vals status-map))
                (throw (ex-info "Encountered unhealthy node during recovery"
                                {:retryable true
                                 :status-maps status-maps}))))

          ;; Assert that all nodes agree on which nodes are in the cluster and
          ;; that we got node info from all nodes in the cluster. Note  that
          ;; this may not be the case if nodes have been removed from the
          ;; cluster during a failure scenario and ns_server hasn't had a
          ;; chance to sync up the nodes.
          node-lists (map #(sort (keys %)) (vals status-maps))
          _ (if-not (apply = (sort (keys status-maps)) node-lists)
              (throw (ex-info "Cluster status inconsistent between nodes"
                              {:retryable true
                               :status-maps status-maps})))

          nodes-in-cluster (first node-lists)

          ;; Determine a healthy node that all nodes agree has not been failed
          ;; over.
          healthy-node (loop [[node & nodes] nodes-in-cluster]
                         (if (and (every? #(= (:clusterMembership (% node)) "active")
                                          (vals status-maps))
                                  (contains? status-maps node))
                           node
                           (if nodes
                             (recur nodes)
                             (throw (ex-info "No healthy node found"
                                             {:retryable true
                                              :status-maps status-maps})))))

          ;; Determine which nodes need recovery. If any status-map indicates
          ;; a node is inactiveFailed we need to recover the node. To check this,
          ;; iterate over all status maps for each node in the cluster.
          recovery-nodes (keep (fn [node] (some #(if (= (:clusterMembership (% node))
                                                        "inactiveFailed")
                                                   node)
                                                (vals status-maps)))
                               nodes-in-cluster)]
      (if (not-empty recovery-nodes)
        (do
          (info "Following nodes will be recovered:" recovery-nodes)
          (doseq [target recovery-nodes]
            (util/rest-call healthy-node
                            "/controller/setRecoveryType"
                            {:otpNode (util/get-node-name target)
                             :recoveryType (name (:recovery-type op))}))
          (c/on healthy-node
                (util/rebalance nodes-in-cluster nil)))
        (info "No recovery necessary"))
      (assoc op :value recovery-nodes))

    (catch Exception e
      (if (and (pos? retry-count)
               (:retryable (ex-data e)))
        (do
          (Thread/sleep 2000)
          (retry (dec retry-count)))
        (do
          (error "Out of retries or non-retryable error occurred during recovery."
                 "Exception was" e)
          (throw e))))))

(defn isolate-completely
  "Introduce a network partition that each targeted node is isolated from all
  other nodes in cluster."
  [test op]
  (assert (= (:f op) :isolate-completely))
  (let [isolate-nodes ((:targeter op) test op)
        other-nodes (set/difference (set (:nodes test)) (set isolate-nodes))
        partitions (conj (partition 1 isolate-nodes) other-nodes)
        grudge (nemesis/complete-grudge partitions)]
    (info "Applying grudge:" grudge)
    (net/drop-all! test grudge)
    (assoc op :value partitions)))

(defn heal-network
  "Remove all active grudges from the network such that all nodes can
  communicate again."
  [test op]
  (assert (= (:f op) :heal-network))
  (with-retry [retry-count 5]
    (net/heal! (:net test) test)
    (catch RuntimeException e
      (warn "Failed to heal network," retry-count "retries remaining")
      (if (pos? retry-count)
        (retry (dec retry-count))
        (throw (RuntimeException. "Failed to heal network" e)))))
  (assoc op :value :healed))

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
        disk-condition    (if (nil? (:disk filter-conditions)) (set [:normal :killed :slowed]) (set (:disk filter-conditions)))
        server-group-condition (set (:server-group filter-conditions))
        cluster-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? cluster-condition (get-in v [:state :cluster]))] k))
        network-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? network-condition (get-in v [:state :network]))] k))
        node-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? node-condition (get-in v [:state :node]))] k))
        disk-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? disk-condition (get-in v [:state :disk]))] k))
        server-group-match-nodes (select-keys node-states (for [[k v] node-states :when (contains? server-group-condition (get-in v [:state :server-group]))] k))
        matching-nodes (set/intersection (set (keys cluster-match-nodes)) (set (keys network-match-nodes)) (set (keys node-match-nodes)) (set (keys disk-match-nodes)))
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
        server-group-json (json/parse-string server-group-info true)
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
        (info "Nemesis setup has started...")
        (reset! nodes (:nodes test))
        (reset! node-states (reduce #(assoc %1 %2 {:state {:cluster :active :network :connected :node :running :disk :normal}}) {} (:nodes test)))
        (when (:server-groups-enabled test)
          (info "inspecting server groups...")
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
            :failover (failover test op)
            :recover (recover test op)
            :isolate-completely (isolate-completely test op)
            :heal-network (heal-network test op)

            :wait-for-autofailover
            (let [target (first target-nodes)
                  initial-count (util/get-autofailover-info target :count)
                  final-count (inc initial-count)
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
                      (update-node-state node-states node-key {:cluster :failed})))))
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
                (update-node-state node-states target-node {:server-group (util/get-node-group target-node)}))
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
                :same-nodes (doseq [node target-nodes] (util/kill-process node :memcached)))
              ;; Wait for rebalance to quit, swallowing rebalance failure
              (try @rebalance (catch Exception e (warn "Rebalance failed")))
              (assoc op :value (str "Rebalance failed for nodes: " (str target-nodes))))

            :kill-process
            (let [process (:process f-opts)]
              (case process
                :memcached
                (do
                  (doseq [node target-nodes] (util/kill-process node :memcached))
                  (info "cluster state: " @node-states)
                  (assoc op :value [:killed :memcached target-nodes]))
                :ns-server
                (do
                  (doseq [node target-nodes] (util/kill-process node :ns-server))
                  (info "cluster state: " @node-states)
                  (assoc op :value [:killed :ns_server target-nodes]))
                :babysitter
                (do
                  (doseq [node target-nodes] (util/kill-process node :babysitter))
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

            :fail-disk
            (do
              (c/on-many
               target-nodes
               (c/su (c/exec :dmsetup :wipe_table :cbdata :--noflush :--nolockfs)
                      ;; Drop buffers. Since most of our tests use little data we can read
                      ;; everything from the filesystem level buffer despite the block device
                      ;; returning errors.
                     (c/exec :echo "3" :> "/proc/sys/vm/drop_caches")))
              (doseq [target target-nodes]
                (update-node-state node-states target {:disk :killed}))
              (info "cluster state: " @node-states)
              (assoc op :value [:disk-failed target-nodes]))

            :slow-disk
            (do
              (c/on-many
               target-nodes
                ;; Load a new (inactive) table that delays all disk IO by 25ms.
               (c/su (c/exec :dmsetup :load :cbdata :--table
                             (c/lit "'0 1048576 delay /dev/loop0 0 25 /dev/loop0 0 25'"))
                     (c/exec :dmsetup :resume :cbdata)))
              (doseq [target target-nodes]
                (update-node-state node-states target {:disk :slowed}))
              (info "cluster state: " @node-states)
              (assoc op :value [:disk-slowed target-nodes]))

            :reset-disk
            (do
              (c/on-many
               target-nodes
               (c/su (c/exec :dmsetup :load :cbdata :--table
                             (c/lit "'0 1048576 linear /dev/loop0 0'"))
                     (c/exec :dmsetup :resume :cbdata)))
              (doseq [target target-nodes]
                (update-node-state node-states target {:disk :normal}))
              (info "cluster state: " @node-states)
              (assoc op :value [:disk-reset target-nodes]))

            :noop
            (do
              (info "cluster state: " @node-states)
              (assoc op :value "noop")))))

      (teardown! [this test]))))

