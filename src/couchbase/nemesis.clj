(ns couchbase.nemesis
  (:require [clojure
             [set :as set]]
            [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase [util :as util]]
            [dom-top.core :as domTop]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]
             [net :as net]]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]))

;; Targeter functions

(defn basic-nodes-targeter
  "A basic targeter that selects :target-count (default 1) random nodes"
  [testData op]
  (->> (:nodes testData)
       (shuffle)
       (take (:target-count op 1))))

(defn target-all-test-nodes
  "A targeter that always targets all the nodes in the test"
  [testData op]
  (:nodes testData))

(defn start-stop-targeter
  "Create a targeter functions with shared state, such that an op :action can
  be classified as either :start or :stop. Once a node has been targeted by a
  :start operation, it can only receive a :stop operation. Likewise a stop
  operation can only be received by a node that previously received a :start.
  Once a targeted node has received a :stop, it can again receive a :start."
  []
  (let [started (atom #{})]
    (fn start-stop-targeter-invoke [testData op]
      (case (:target-action op)
        :start (let [target-count (:target-count op 1)
                     all-nodes (set (:nodes testData))
                     possible-targets (set/difference all-nodes @started)
                     targets (take target-count (shuffle possible-targets))]
                 (if (not= target-count (count targets))
                   (throw (ex-info "Not enough undisrupted nodes to start"
                                   {:started @started})))
                 (swap! started set/union (set targets))
                 targets)
        :stop (let [target-count (:target-count op 1)
                    targets (take target-count (shuffle @started))]
                (if (not= target-count (count targets))
                  (throw (ex-info "Not enought disrupted nodes to stop"
                                  {:started @started})))
                (swap! started set/difference (set targets))
                targets)
        :swap (let [target-count (:target-count op 1)
                    all-nodes (set (:nodes testData))
                    stop-targets (take target-count (shuffle @started))
                    start-targets (->> (set/difference all-nodes @started)
                                       (shuffle)
                                       (take target-count))]
                (if (not= target-count (count stop-targets) (count start-targets))
                  (throw (ex-info "Not enough nodes to swap"
                                  {:started @started})))
                (swap! started set/difference (set stop-targets))
                (swap! started set/union (set start-targets))
                [stop-targets start-targets])))))

;; Nemesis functions

(defn failover
  "Using the rest-api trigger a failover of the target nodes"
  [testData op]
  (assert (= (:f op) :failover))
  (let [fail-type (:failover-type op)
        target-nodes ((:targeter op) testData op)
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
  [testData op]
  (assert (= (:f op) :recover))
  (domTop/with-retry [retry-count 10]
    (let [status-maps (util/get-node-info-map testData)

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
  [testData op]
  (assert (= (:f op) :isolate-completely))
  (let [isolate-nodes ((:targeter op) testData op)
        other-nodes (set/difference (set (:nodes testData)) (set isolate-nodes))
        partitions (conj (partition 1 isolate-nodes) other-nodes)
        grudge (nemesis/complete-grudge partitions)]
    (info "Applying grudge:" grudge)
    (net/drop-all! testData grudge)
    (assoc op :value partitions)))

(defn heal-network
  "Remove all active grudges from the network such that all nodes can
  communicate again."
  [testData op]
  (assert (= (:f op) :heal-network))
  (domTop/with-retry [retry-count 5]
    (net/heal! (:net testData) testData)
    (catch RuntimeException e
      (warn "Failed to heal network," retry-count "retries remaining")
      (if (pos? retry-count)
        (retry (dec retry-count))
        (throw (RuntimeException. "Failed to heal network" e)))))
  (assoc op :value :healed))

(defn rebalance-out
  "Rebalance nodes out of the cluster."
  [testData op]
  (assert (= (:f op) :rebalance-out))
  (let [eject-nodes ((:targeter op) testData op)
        cluster-nodes (util/get-cluster-nodes testData)]
    (c/on (first (set/difference (set cluster-nodes) (set eject-nodes)))
          (util/rebalance cluster-nodes eject-nodes))
    (assoc op :value eject-nodes)))

(defn rebalance-in
  "Rebalance node in to the cluster."
  [testData op]
  (assert (= (:f op) :rebalance-in))
  (let [add-nodes ((:targeter op) testData op)
        add-options (:add-opts op)
        cluster-nodes (util/get-cluster-nodes testData)
        new-cluster-nodes (set/union (set cluster-nodes) (set add-nodes))]
    (c/on (first cluster-nodes)
          (util/add-nodes add-nodes add-options)
          (util/rebalance new-cluster-nodes))
    (assoc op :value add-nodes)))

(defn swap-rebalance
  "Swap in new nodes for existing nodes."
  [testData op]
  (assert (= (:f op) :swap-rebalance))
  (let [[add-nodes remove-nodes] ((:targeter op) testData op)
        add-count (count add-nodes)
        add-options (:add-opts op)
        cluster-nodes (util/get-cluster-nodes testData)
        static-nodes (set/difference (set cluster-nodes) (set remove-nodes))]
    (c/on (first static-nodes)
          (util/add-nodes add-nodes add-options)
          (util/rebalance (set/union add-nodes cluster-nodes)
                          remove-nodes))
    (assoc op :value {:in add-nodes :out remove-nodes})))

(defn fail-rebalance
  "Start rebalancing nodes out of the cluster, then kill those node to cause
  a rebalance failure."
  [testData op]
  (assert (= (:f op) :fail-rebalance))
  (let [target-nodes ((:targeter op) testData op)
        cluster-nodes (util/get-cluster-nodes testData)
        rest-target (first (set/difference (set cluster-nodes)
                                           (set target-nodes)))
        rebalance-f (future (c/on rest-target
                                  (util/rebalance cluster-nodes target-nodes)))]
    ;; Sleep 4 seconds to allow the rebalance to start
    (Thread/sleep 4000)
    ;; Kill memcached on the target nodes to cause the failure
    (doseq [target target-nodes]
      (util/kill-process target :memcached))
    ;; Wait for the rebalance to quit, swallowing the exception
    (try @rebalance-f (catch Exception e (info "Expected rebalance failure detected")))
    (assoc op :value target-nodes)))

(defn kill-process
  "Kill a process on the targeted noded"
  [testData op]
  (assert (= (:f op) :kill-process))
  (let [target-nodes ((:targeter op) testData op)
        process (:kill-process op)]
    (doseq [node target-nodes] (util/kill-process node process))
    (assoc op :value target-nodes)))

(defn start-process
  "Restart the Couchbase Server process"
  [testData op]
  (assert (= (:f op) :start-process))
  (let [target-nodes ((:targeter op) testData op)
        exec-path (str (:install-path testData) "/bin/couchbase-server")]
    (c/on-many
     target-nodes
     (try
       (c/su (c/exec :systemctl :start :couchbase-server))
       (catch Exception e
         (c/ssh* {:cmd (str "nohup " exec-path " -- -noinput >> /dev/null 2>&1 &")})))
     (util/wait-for-daemon))
    (assoc op :value target-nodes)))

(defn slow-dcp-client
  "Slow down the set workload DCP client"
  [testData op]
  (assert (= (:f op) :slow-dcp-client))
  (assert (:dcp-set-read testData))
  (reset! (->> testData :client :dcpclient :slow) true)
  op)

(defn reset-dcp-client
  "Reset the set workload DCP client"
  [testData op]
  (assert (= (:f op) :reset-dcp-client))
  (assert (:dcp-set-read testData))
  (reset! (->> testData :client :dcpclient :slow) false)
  op)

(defn trigger-compaction
  "Trigger compaction on the cluster"
  [testData op]
  (assert (= (:f op) :trigger-compaction))
  (let [cluster-nodes (util/get-cluster-nodes testData)]
    (util/rest-call (rand-nth cluster-nodes)
                    "/pools/default/buckets/default/controller/compactBucket"
                    "")))

(defn fail-disk
  "Simulate a disk failure on the targeted nodes"
  [testData op]
  (assert (= (:f op) :fail-disk))
  (let [target-nodes ((:targeter op) testData op)]
    (c/on-many
     target-nodes
     ;; Load a new (inactive) table that delays all disk IO by 25ms.
     (c/su (c/exec :dmsetup :load :cbdata :--table
                   (c/lit "'0 1048576 delay /dev/loop0 0 25 /dev/loop0 0 25'"))
           (c/exec :dmsetup :resume :cbdata)))
    (assoc op :value target-nodes)))

(defn slow-disk
  "Slow down disk operations on the targeted nodes"
  [testData op]
  (assert (= (:f op) :slow-disk))
  (let [target-nodes ((:targeter op) testData op)]
    (c/on-many
     target-nodes
     ;; Load a new (inactive) table that delays all disk IO by 25ms.
     (c/su (c/exec :dmsetup :load :cbdata :--table
                   (c/lit "'0 1048576 delay /dev/loop0 0 25 /dev/loop0 0 25'"))
           (c/exec :dmsetup :resume :cbdata)))
    (assoc op :value target-nodes)))

(defn reset-disk
  "Reset the virtual disk on the targeted nodes"
  [testData op]
  (assert (= (:f op) :reset-disk))
  (let [target-nodes ((:targeter op) testData op)]
    (c/on-many
     target-nodes
     (c/su (c/exec :dmsetup :load :cbdata :--table
                   (c/lit "'0 1048576 linear /dev/loop0 0'"))
           (c/exec :dmsetup :resume :cbdata)))
    (assoc op :value target-nodes)))

(defn couchbase
  "The Couchbase nemesis represents operations that can be taken against a
  Couchbase Server cluster. Each invoke can select which nodes to act upon
  by providing a targeter function. It is the responsibility of the caller
  to ensure that the chain of events specified by the targeter function of
  successive invokation is valid."
  []
  (reify nemesis/Nemesis
    (setup! [this testData] this)

    (invoke! [this testData op]
      (case (:f op)
        :failover (failover testData op)
        :recover (recover testData op)
        :isolate-completely (isolate-completely testData op)
        :heal-network (heal-network testData op)

        :rebalance-out (rebalance-out testData op)
        :rebalance-in (rebalance-in testData op)
        :swap-rebalance (swap-rebalance testData op)
        :fail-rebalance (fail-rebalance testData op)

        :kill-process (kill-process testData op)
        :start-process (start-process testData op)

        :slow-dcp-client (slow-dcp-client testData op)
        :reset-dcp-client (reset-dcp-client testData op)

        :trigger-compaction (trigger-compaction testData op)

        :fail-disk (fail-disk testData op)
        :slow-disk (slow-disk testData op)
        :reset-disk (reset-disk testData op)

        :noop op))

    (teardown! [this testData])))

