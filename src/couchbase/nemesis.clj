(ns couchbase.nemesis
  (:require [clojure [set    :as set]
                     [string :as string]]
            [clojure.tools.logging :refer :all]
            [couchbase [util      :as util]]
            [jepsen    [control   :as c]
                       [generator :as gen]
                       [nemesis   :as nemesis]
                       [net :as net]]
            [cheshire.core :refer :all]))

(defn disconnect-two
  "Introduce a partition that prevents two nodes from communicating"
  [first second]
  (let [targeter (fn [nodes] {first #{second}, second #{first}})]
    (nemesis/partitioner targeter)))

(defn failover-basic
  "Actively failover a node through the rest-api. Supports :delta, or
  :full recovery on nemesis stop."
  ([]         (failover-basic rand-nth :delta))
  ([targeter] (failover-basic targeter :delta))
  ([targeter recovery-type]
   (nemesis/node-start-stopper
     targeter
     (fn start [test node]
       (let [endpoint "/controller/failOver"
             params   (str "otpNode=ns_1@" node)]
         (util/rest-call endpoint params)
         [:failed-over node]))

     (fn stop [test node]
       (util/rest-call "/controller/setRecoveryType"
                       (format "otpNode=%s&recoveryType=%s"
                               (str "ns_1@" node)
                               (name recovery-type)))
       (util/rebalance (test :nodes))))))

(defn partition-then-failover
  "Introduce a partition such that two nodes cannot communicate, then failover
  one of those nodes. This Nemesis is only used as a constructor, during setup
  it replaces itself with a new nemesis constructed by jepsen.nemesis/compose"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (let [[first, second]    (->> (:nodes test)
                                    (shuffle)
                                    (take 2))
            disconnect-nemesis (disconnect-two first second)
            failover-nemesis   (failover-basic (constantly first))
            combined-nemesis   (nemesis/compose
                                 {{:start-partition :start} disconnect-nemesis
                                  {:start-failover  :start} failover-nemesis})]
        (info "Nemesis is going to partition" first "and" second
              "then failover" first)
        (nemesis/setup-compat! combined-nemesis test nil)))

    ; These are never called, since we replace this nemesis during setup
    (invoke! [this test op])
    (teardown! [this test] this)))

(defn graceful-failover
  "Gracefully fail over a random node upon start. Perform :delta or
  :full node recovery on nemesis stop"
  ([]         (graceful-failover rand-nth :delta))
  ([targeter] (graceful-failover targeter :delta))
  ([targeter recovery-type]
   (nemesis/node-start-stopper targeter
     (fn start [test target]
       (let [endpoint   "/controller/startGracefulFailover"
             params     (str "otpNode=ns_1@" target)
             get-status #(util/rest-call "/pools/default/rebalanceProgress" nil)]
         (util/rest-call endpoint params)

         (loop [status (get-status)]
           (if (not= status "{\"status\":\"none\"}")
             (do
               (info "Graceful failover status" status)
               (Thread/sleep 1000)
               (recur (get-status)))
           (info "Graceful failover complete")))
         [:gracefully-failed-over target]))

     (fn stop [test node]
       (util/rest-call "/controller/setRecoveryType"
                       (format "otpNode=%s&recoveryType=%s"
                               (str "ns_1@" node)
                               (name recovery-type)))
       (util/rebalance (test :nodes))))))

(defn rebalance-out-in
  "Rebalance nodes of out of and back into the cluster"
  []
  (let [ejected (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this test]
        (reset! ejected #{})
        this)
      (invoke! [this test op]
        (case (:f op)
          :rebalance-out   (let [amount    (or (:count op) 1)
                                 all-nodes (set (test :nodes))
                                 remaining (set/difference all-nodes @ejected)
                                 eject     (->> (seq remaining)
                                                (shuffle)
                                                (take amount)
                                                (set))]
                             (swap! ejected set/union eject)
                             (util/rebalance remaining eject)
                             (assoc op :value (str "Removed: " eject)))
          :rebalance-in    (let [amount     (or (:count op) 1)
                                 all-nodes  (set (test :nodes))
                                 in-cluster (set/difference all-nodes @ejected)
                                 add        (->> (seq @ejected)
                                                 (shuffle)
                                                 (take amount)
                                                 (set))]
                             (swap! ejected set/difference add)
                             (c/on (first in-cluster) (util/add-nodes add))
                             (util/rebalance (set/union in-cluster add))
                             (assoc op :value (str "Added: " add)))))
      (teardown! [this test]))))

(defn swap-rebalance
  "Upon each invocation swap rebalance $count nodes. In order to have free nodes
  to rebalance in, we rebalance out $count nodes upon nemesis setup"
  [count]
  (let [ejected (atom #{})]
    (reify nemesis/Nemesis
      (setup! [this test]
        (reset! ejected (->> (test :nodes)
                             (shuffle)
                             (take count)
                             (set)))
        (util/rebalance (test :nodes) @ejected)
        this)
      (invoke! [this test op]
        (if-not (= (:f op) :swap)
          (throw (RuntimeException. "Op for swap-rebalance nemesis must be :swap")))
        (let [nodes      (set (test :nodes))
              in-cluster (set/difference nodes @ejected)
              to-remove  (->> in-cluster
                              (shuffle)
                              (take count)
                              (set))]
          (c/on (first in-cluster) (util/add-nodes @ejected))
          (util/rebalance nodes to-remove)
          (reset! ejected to-remove))
        (assoc op :type :info :status :done))
      (teardown! [this test] nil))))

(defn fail-rebalance
  "Start a rebalance, then cause it to fail by killing memcached on some nodes.
  Since we ensure the rebalance fails, no nodes should ever actually leave the
  cluster, so we don't need to keep track of the cluster status."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [thid test op]
      (case (:f op)
        :start (let [count      (or (op :count) 1)
                     eject      (->> test :nodes shuffle (take count))
                     rebalance  (future (util/rebalance (:nodes test) eject))]
                 ;; Sleep between 2 and 4 seconds to allow the rebalance to start
                 (Thread/sleep (+ (* (rand) 2000) 2000))
                 ;; Kill memcached on a different random collection of nodes
                 (c/on-many (->> test :nodes shuffle (take count))
                            (c/su (c/exec :pkill :-9 :memcached)))
                 ;; Wait for rebalance to quit, swallowing rebalance failure
                 (try @rebalance (catch Exception e))
                 (assoc op :value :ok))))
    (teardown! [this test])))

(defn kill-memcached
  "Upon invocation kill memcached to simulate a crash"
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :kill (let [count   (or (op :count) 1)
                    targets (->> (test :nodes)
                                 (shuffle)
                                 (take count))]
                (c/on-many targets (c/su (c/exec :pkill :-9 :memcached)))
                (assoc op :value [:killed :memcached targets]))))
    (teardown! [this test] nil)))

(defn kill-ns_server
  "Upon invocation kill ns_server"
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :kill (let [count   (or (op :count) 1)
                    targets (->> (test :nodes)
                                 (shuffle)
                                 (take count))]
                (c/on-many targets
                           (c/su (c/exec :bash :-c "kill -9 $(pgrep beam.smp | tail -n +2)")))
                (assoc op :value [:killed :ns_server targets]))))
    (teardown! [this test] nil)))

(defn slow-dcp [DcpClient]
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :start (do
                 (reset! (:slow DcpClient) true)
                 (assoc op :type :info))
        :stop  (do
                 (reset! (:slow DcpClient) false)
                 (assoc op :type :info))))
    (teardown! [this test]
      (reset! (:slow DcpClient) false))))

(defn trigger-compaction []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (assert (= (:f op) :compact))
      (util/rest-call (rand-nth (test :nodes))
                      "/pools/default/buckets/default/controller/compactBucket"
                      "")
      op)
    (teardown! [this test])))

(defn disk-failure
  "Simulate a disk failure on the data path.

  This nemesis will not work correctly with docker containers"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (let [path        (or (->> test :package :path) "/opt/couchbase")
            server-path (str path "/bin/couchbase-server")
            data-path   (str path "/var/lib/couchbase/data")]
        (c/with-test-nodes test
          (c/su (c/exec :dd "if=/dev/zero" "of=/tmp/cbdata.img" "bs=1M" "count=512")
                (c/exec :losetup "/dev/loop0" "/tmp/cbdata.img")
                (c/exec :dmsetup :create :cbdata :--table (c/lit "'0 1048576 linear /dev/loop0 0'"))
                (c/exec :mkfs.ext4 "/dev/mapper/cbdata")
                (c/exec server-path :-k)
                (c/exec :mv data-path "/tmp/cbdata")
                (c/exec :mkdir data-path)
                (c/exec :mount "/dev/mapper/cbdata" data-path)
                (c/exec :chmod "a+rwx" data-path)
                (c/exec :mv (c/lit "/tmp/cbdata/*") data-path)
                (c/exec :rm :-r "/tmp/cbdata"))
          (c/ssh* {:cmd (str "nohup " server-path " -- -noinput >> /dev/null 2>&1 &")})
          (util/wait-for-daemon)
          (util/wait-for-warmup))
        this))
    (invoke! [this test op]
      (case (:f op)
        :start (let [fail (->> test :nodes shuffle (take (op :count)))]
                 (c/on-many
                  fail
                  (c/su (c/exec :dmsetup :wipe_table :cbdata :--noflush :--nolockfs)
                        ;; Drop buffers. Since most of our tests use little data we can read
                        ;; everything from the filesystem level buffer despite the block device
                        ;; returning errors.
                        (c/exec :echo "3" :> :/proc/sys/vm/drop_caches)))
                 (assoc op :value [:failed fail]))))

    (teardown! [this test]
      (c/with-test-nodes test
        (c/su (c/exec :umount :-l "/dev/mapper/cbdata")
              (c/exec :dmsetup :remove :-f "/dev/mapper/cbdata")
              (c/exec :losetup :-d "/dev/loop0")
              (c/exec :rm "/tmp/cbdata.img"))))))

(defn filter-nodes
  "This function will take in node-state atom and targeter-opts. Target conditions will be extracted from
  targeter-opts and used to filter nodes represented in node-states. This function allows target conditions to
  be keyword, vector of keywords, or vector of vectors of keywords. Returns a vector of nodes

  A vector of keywords will grab all nodes that match any condition, [:active :failed] will return all
  nodes that are either :active or :failed without regard for other state keywords

  A vector of vectors will grab all nodes that match exactly the vectors condition for all vectors,
  [[:active :connected] [:failed :partitioned]] will return the union of all active connected nodes and
  failed partitioned nodes"
  [node-states targeter-opts]
  (let [node-conditions (:condition targeter-opts)]
    (cond
      ;; keyword conditions should not filter nodes based on state, use for special cases
      (keyword? node-conditions)
      (case node-conditions
        :all (vec (keys node-states)))

      (vector? node-conditions)
      (loop [node-set #{}
             node-conditions node-conditions]
        (if (not-empty node-conditions)
          (let [current-condition (first node-conditions)
                filtered-node-map  (cond
                                     (= (type current-condition) clojure.lang.Keyword)
                                     (select-keys node-states (for [[k v] node-states :when (contains? (set (:state v)) current-condition)] k))
                                     (= (type current-condition) clojure.lang.PersistentVector)
                                     (select-keys node-states (for [[k v] node-states :when (= (set (:state v)) (set current-condition))] k)))
                filtered-nodes (set (keys filtered-node-map))
                new-node-set (set/union filtered-nodes node-set)
                remaining-conditions (remove #(= current-condition %) node-conditions)]
            (recur new-node-set remaining-conditions))
          (vec node-set))))))

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
    (vec target-seq))
  )

(defn get-targets
  "This function takes in an atom representing node states and a targeter-opts map. The function will first
  apply a filter to the list of nodes based on node state, then it will target a subset of the filtered nodes
  and return the select nodes as a vector"
  [node-states targeter-opts]
  (let [filtered-nodes (filter-nodes node-states targeter-opts)
        target-nodes (apply-targeter filtered-nodes targeter-opts)]
    target-nodes
    )
  )

(defn create-grudge
  "This function will create a partition grudge to be based to Jepsen. Partitions look like [(n1) (n2 n3) (n4)].
  Each sequence represents partitions such that only nodes within the same sequence can communicate."
  [partition-type target-nodes all-nodes]
  (case partition-type
    :isolate-completely
    (let [complement-nodes (seq (set/difference (set all-nodes) (set target-nodes)))
          partitions (conj (partition 1 target-nodes) complement-nodes)
          grudge (nemesis/complete-grudge partitions)]
      grudge)
    )
  )

(defn get-new-state
  "This function takes in a atom of node states, a target node, a state to remove from the target node, and
  a state to add to the target node."
  [node-states target old-state new-state]
  (let [node-state (get-in @node-states [target :state])
        old-state-removed (set (remove #(= old-state %) node-state))
        new-state (vec (conj old-state-removed new-state))]
    new-state))

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
        (reset! node-states (reduce #(assoc %1 %2 {:state [:active :connected]}) {} (:nodes test)))
        this)

      (invoke! [this test op]
        (let [f-opts (:f-opts op)
              targeter-opts (:targeter-opts op)
              target-nodes (if (nil? targeter-opts) @nodes (get-targets @node-states targeter-opts))]
          (case (:f op)

            :failover
            (let [failover-type (:failover-type f-opts)
                  endpoint (case failover-type
                             :hard "/controller/failOver"
                             :graceful "/controller/startGracefulFailover")]
              (assert (< (count target-nodes) (count (filter-nodes @node-states {:condition [[:active :connected]]}))) "failover count must be less than total active node count")
              (doseq [target target-nodes]
                (util/rest-call target endpoint (str "otpNode=ns_1@" target))
                (if (= failover-type :graceful)
                  (util/wait-for #(util/rest-call target "/pools/default/rebalanceProgress" nil) "{\"status\":\"none\"}"))
                (swap! node-states assoc-in [target :state] (get-new-state node-states target :active :failed)))
              (assoc op :value :failover-complete))

            :recover
            (let [recovery-type (:recovery-type f-opts)
                  active-nodes (vec (filter-nodes @node-states {:condition [[:active :connected]]}))
                  failed-nodes (vec (filter-nodes @node-states {:condition [[:failed :connected]]}))
                  eject-nodes (vec (set/difference (set failed-nodes) (set target-nodes)))
                  rebalance-nodes (concat active-nodes failed-nodes)]
              (assert (<= (count target-nodes) (count failed-nodes)) "recovery count must be less or equal to the total failed node count")
              (doseq [target target-nodes]
                (let [params (str (format  "otpNode=%s&recoveryType=%s" (str "ns_1@" target) (name recovery-type)))]
                  (util/rest-call target "/controller/setRecoveryType" params)))
              (util/rebalance rebalance-nodes)
              (doseq [target target-nodes]
                (swap! node-states assoc-in [target :state] (get-new-state node-states target :failed :active)))
              (doseq [target eject-nodes]
                (swap! node-states assoc-in [target :state] (get-new-state node-states target :failed :ejected)))
              (assoc op :value :recovery-complete))

            :partition-network
            (let [partition-type (:partition-type f-opts)
                  grudge (create-grudge partition-type target-nodes @nodes)]
              (net/drop-all! test grudge)
              (doseq [target target-nodes]
                (swap! node-states assoc-in [target :state] (get-new-state node-states target :connected :partitioned)))
              (assoc op :value [:isolated grudge]))

            :heal-network
            (do
              (net/heal! (:net test) test)
              (doseq [target target-nodes]
                (swap! node-states assoc-in [target :state] (get-new-state node-states target :partitioned :connected)))
              (assoc op :value :network-healed))

            :wait-for-autofailover
            (let [autofailover-count (atom 0)
                  target (first target-nodes)
                  node-info-before (util/get-node-info target)]
              (util/wait-for #(util/get-autofailover-info target :count) 1)
              (let [node-info-after (util/get-node-info target)]
                (doseq [node-info node-info-before]
                  (let [node-key (key node-info)
                        state-before (get-in node-info-before [node-key :clusterMembership])
                        state-after (get-in node-info-after [node-key :clusterMembership])
                        active-before (= state-before "active")
                        failed-after (= state-after "inactiveFailed")]
                    (if (and active-before failed-after)
                      (do
                        (swap! node-states assoc-in [node-key :state] (get-new-state node-states node-key :active :failed))
                        (swap! autofailover-count inc)))
                    )
                  )
                )
              (assert (= @autofailover-count 1) (str "autofailover-count != 1"))
              (assoc op :value :autofailover-complete))
            )))

      (teardown! [this test])

      )))

