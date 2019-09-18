(ns couchbase.workload
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase [checker   :as cbchecker]
             [clients   :as clients]
             [cbclients :as cbclients]
             [nemesis   :as cbnemesis]
             [util      :as util]]
            [jepsen [checker     :as checker]
             [generator   :as gen]
             [independent :as independent]
             [nemesis     :as nemesis]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.time :as nt]
            [knossos.model :as model]))

;; =================
;; HELPER GENERATORS
;; =================

(gen/defgenerator while-atom-continue
  [gen]
  [gen]
  (op [_ test process]
      (if (= @(:control-atom test) :continue)
        (gen/op gen test process))))

(gen/defgenerator set-atom-stop
  []
  []
  (gen/op [_ test process]
          (compare-and-set! (:control-atom test) :continue :stop)
          nil))

(gen/defgenerator start-timeout
  [timeout]
  [timeout]
  (op [_ test process]
      (future (do (Thread/sleep timeout)
                  (if (compare-and-set! (:control-atom test) :continue :abort)
                    (error "TEST TIMEOUT EXCEEDED, ABORTING"))))
      nil))

(defn do-n-nemesis-cycles
  ([n nemesis-seq client-gen] (do-n-nemesis-cycles n [] nemesis-seq [] client-gen))
  ([n nemesis-pre-seq nemesis-seq nemesis-post-seq client-gen]
   (let [nemesis-gen  (gen/seq (flatten [(start-timeout. (* n 1000 60 30))
                                         nemesis-pre-seq
                                         (repeat n nemesis-seq)
                                         nemesis-post-seq
                                         (gen/sleep 3)
                                         (set-atom-stop.)]))
         rclient-gen  (while-atom-continue. client-gen)]
     (gen/nemesis nemesis-gen rclient-gen))))

(defn rate-limit
  [rate g]
  (if (not= 0 rate)
    (gen/stagger (/ rate) g)
    g))

;; =============
;; Helper macros
;; =============

(defmacro let-and-merge
  "Take a map and pairs of parameter-name parameter-value. Merge these pairs into
  the map while also binding the names to their values to allow parameters values
  dependent on previous values."
  ([map] map)
  ([map param value & more] `(let [~param ~value
                                   ~map (assoc ~map (keyword (name '~param)) ~value)]
                               (let-and-merge ~map ~@more))))

(defmacro with-register-base
  "Apply a set of shared parameters used across the register workloads before
  merging the custom parameters"
  ([opts & more]
   `(let-and-merge
     ~opts
     ~'cycles                (~opts :cycles 1)
     ~'cas                   (~opts :cas)
     ~'node-count            (~opts :node-count (count (~opts :nodes)))
     ~'nodes                 (if (>= (count (~opts :nodes)) ~'node-count)
                               (vec (take ~'node-count (~opts :nodes)))
                               (assert false "node-count greater than available nodes"))
     ~'server-group-count    (~opts :server-group-count 1)
     ~'transactions          (~opts :transactions false)
     ~'mixed-txns            (and ~'transactions
                                  (~opts :mixed-txns false))
     ~'max-txn-size          (~opts :max-txn-size 3)
     ~'rate                  (~opts :rate 1/3)
     ~'txn-bucket-count      (~opts :txn-bucket-count 1)
     ~'txn-doc-count         (~opts :txn-doc-count 3)
     ~'doc-count             (if ~'transactions ~'txn-bucket-count  (~opts :doc-count 40))
     ~'doc-threads           (if ~'transactions (~opts :doc-threads (* ~'txn-doc-count 2)) (~opts :doc-threads 3))
     ~'concurrency           (* ~'doc-count ~'doc-threads)
     ~'pool-size             (~opts :pool-size 6)
     ~'autofailover-timeout  (~opts :autofailover-timeout 6)
     ~'autofailover-maxcount (~opts :autofailover-maxcount 3)
     ~'durability            (~opts :durability [100 0 0 0])
     ~'client                (clients/register-client)
     ~'model                 (if ~'transactions
                               (model/multi-register (zipmap (range ~'txn-doc-count) (repeat :nil)))
                               (model/cas-register :nil))
     ~'control-atom          (atom :continue)
     ~'checker               (checker/compose
                              (merge
                               {:indep (independent/checker
                                        (checker/compose
                                         {:timeline (timeline/html)
                                          :linear (checker/linearizable {:model ~'model})}))
                                :sanity (cbchecker/sanity-check)}
                               (if (~opts :perf-graphs)
                                 {:perf (checker/perf)})))
     ~@more)))

;; =================
;; HELPER FUNCTIONS
;; =================

(defn random-sample-from-n
  "Returns a random subset of sequence from 0 - n-1"
  [n]
  (let [n-vec (vec (range n))]
    (take (inc (rand-int n)) (shuffle n-vec))))

(defn mixed-txn-gen
  "Generate mixed transactions consisting of both read and write operations"
  [opts]
  (let [gen (fn [_ _]
              ;; Choose a random set of documents in the range 0 to :txn-doc-count, then
              ;; limit to :max-txn-size documents. For each chosen document, choose to
              ;; perform either a read or write operation
              (->> (random-sample-from-n (:txn-doc-count opts))
                   (take (:max-txn-size opts))
                   (mapv (fn [key] (let [op-type (rand-nth [:read :write])
                                         val (when (= op-type :write) (rand-int 50))]
                                     [op-type key val])))
                   (array-map :type :invoke
                              :f :txn
                              :durability-level (util/random-durability-level (:durability opts))
                              :value)))]
    (independent/concurrent-generator (:doc-threads opts)
                                      (range)
                                      (fn [k] (rate-limit (:rate opts) gen)))))

(defn txn-gen
  "Generate a mix of read transactions and write transactions"
  [opts]
  (let [read-gen (fn [_ _]
                   ;; Choose a random set of documents in the range 0 to :txn-doc-count, then
                   ;; limit to :max-txn-size documents. Map each chose document to a read op.
                   (->> (random-sample-from-n (:txn-doc-count opts))
                        (take (:max-txn-size opts))
                        (mapv (fn [k] [:read k nil]))
                        (array-map :type :invoke
                                   :f :txn
                                   :durability-level (util/random-durability-level (:durability opts))
                                   :value)))
        write-gen (fn [_ _]
                    ;; Choose a random set of documents in the range 0 to :txn-doc-count, then
                    ;; limit to :max-txn-size documents. Map each chose document to a write op.
                    (->> (random-sample-from-n (:txn-doc-count opts))
                         (take (:max-txn-size opts))
                         (mapv (fn [k] [:write k (rand-int 50)]))
                         (array-map :type :invoke
                                    :f :txn
                                    :durability-level (util/random-durability-level (:durability opts)))))]
    (independent/concurrent-generator
     (:doc-threads opts)
     (range)
     (fn [k] (rate-limit (:rate opts)
                         (gen/mix [read-gen write-gen]))))))

(defn register-gen
  "Generate standard register operations"
  [opts]
  (let [read-gen (fn [_ _] {:type :invoke
                            :f :read
                            :value nil})
        write-gen (fn [_ _]
                    {:type :invoke
                     :f :write
                     :value (rand-int 50)
                     :replicate-to (:replicate-to opts)
                     :persist-to (:persist-to opts)
                     :durability-level (util/random-durability-level
                                        (:durability opts))})
        cas-gen (fn [_ _]
                  {:type :invoke
                   :f :cas
                   :value [(rand-int 50) (rand-int 50)]
                   :replicate-to (:replicate-to opts)
                   :persist-to (:persist-to opts)
                   :durability-level (util/random-durability-level
                                      (:durability opts))})]
    (independent/concurrent-generator
     (:doc-threads opts)
     (range)
     (fn [k] (rate-limit
              (:rate opts)
              (gen/mix (if (:cas opts)
                         [read-gen write-gen cas-gen]
                         [read-gen write-gen])))))))

(defn client-gen
  "Based on the provided cli options, determine whether to generate mixed
  transactions, transactions or basic register operations for the client;
  then create the corresponding operation generator."
  [opts]
  (cond
    (:mixed-txns opts) (mixed-txn-gen opts)
    (:transactions opts) (txn-gen opts)
    :else (register-gen opts)))

;; ==================
;; Register workloads
;; ==================

(defn register-workload
  "Basic register workload"
  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    nemesis       nemesis/noop
    client-generator      (client-gen opts)
    generator (do-n-nemesis-cycles cycles [(gen/sleep 20)] client-generator)))

(defn partition-workload
  "Paritions the network by isolating nodes from each other, then will recover if autofailover
  happens"
  [opts]
  (with-register-base opts
    replicas       (opts :replicas 1)
    autofailover   (opts :autofailover false)
    server-group-autofailover (opts :server-group-autofailover false)
    disrupt-time   (opts :disrupt-time 20)
    recovery-type  (opts :recovery-type :delta)
    disrupt-count  (opts :disrupt-count 1)
    sg-enabled     (opts :server-groups-enabled)
    server-group-count (if sg-enabled (opts :server-group-count))
    target-server-groups      (if (opts :target-server-groups) (do (assert sg-enabled) true) false)
    random-server-group (if sg-enabled (util/random-server-group server-group-count))
    complementary-server-group (if sg-enabled (util/complementary-server-group server-group-count random-server-group))
    nemesis        (cbnemesis/couchbase)
    client-generator (client-gen opts)
    generator       (do-n-nemesis-cycles
                     cycles
                     [(gen/sleep 5)
                      {:type :info
                       :f :isolate-completely
                       :targeter cbnemesis/basic-nodes-targeter
                       :target-count disrupt-count}
                      (gen/sleep disrupt-time)
                      {:type :info :f :heal-network}
                      {:type :info
                       :f :recover
                       :recovery-type recovery-type}
                      (gen/sleep 10)]
                     client-generator)))

(defn rebalance-workload
  "Rebalance scenarios:
  Sequential Rebalance In/Out - rebalance out a single node at a time until disrupt-count nodes have
                                been rebalanced out. This is followed by the opposite - rebalance
                                back in a single node at a time until disrupt-count nodes have been
                                rebalanced back in.

  Bulk Rebalance In/Out       - rebalance out disrupt-count nodes at once followed by rebalancing in
                                the same node at once.

  Swap Rebalance              - first disrupt-count nodes are rebalanced out of the cluster. Then,
                                these nodes are used to perform a swap rebalance with dirsupt-count
                                nodes that are still in the cluster"

  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    scenario      (opts :scenario)
    disrupt-count (if (= scenario :swap-rebalance)
                    (let [disrupt-count (opts :disrupt-count 1)
                          max-disrupt-count (dec (Math/ceil (double (/ (count (:nodes opts)) 2))))]
                      (assert (<= disrupt-count max-disrupt-count))
                      disrupt-count)
                    (let [disrupt-count (opts :disrupt-count 1)
                          max-disrupt-count (dec (count (:nodes opts)))]
                      (assert (<= disrupt-count max-disrupt-count))
                      disrupt-count))
    autofailover  (opts :autofailover false)
    server-group-autofailover (opts :server-group-autofailover false)
    sg-enabled     (opts :server-groups-enabled)
    server-group-count (if sg-enabled (opts :server-group-count) 0)
    target-server-groups      (if (opts :target-server-groups) (do (assert sg-enabled) true) false)
    random-server-group (if sg-enabled (util/random-server-group server-group-count))
    complementary-server-group (if sg-enabled (util/complementary-server-group server-group-count random-server-group))
    nemesis       (cbnemesis/couchbase)
    targeter      (cbnemesis/start-stop-targeter)
    client-generator (client-gen opts)
    generator     (case scenario
                    :sequential-rebalance-out-in
                    (do-n-nemesis-cycles
                     cycles
                     [(repeatedly
                       disrupt-count
                       (fn [] [(gen/sleep 5)
                               {:type :info
                                :f :rebalance-out
                                :targeter targeter
                                :target-count 1
                                :target-action :start}]))
                      (gen/sleep 5)
                      (repeatedly
                       disrupt-count
                       (fn [] [(gen/sleep 5)
                               {:type :info
                                :f :rebalance-in
                                :targeter targeter
                                :target-count 1
                                :target-action :stop}]))
                      (gen/sleep 5)]
                     client-generator)

                    :bulk-rebalance-out-in
                    (do-n-nemesis-cycles
                     cycles
                     [(gen/sleep 5)
                      {:type :info
                       :f :rebalance-out
                       :targeter targeter
                       :target-count disrupt-count
                       :target-action :start}
                      (gen/sleep 5)
                      {:type :info
                       :f :rebalance-in
                       :targeter targeter
                       :target-count disrupt-count
                       :target-action :stop}
                      (gen/sleep 5)]
                     client-generator)

                    :swap-rebalance
                    (do-n-nemesis-cycles
                     cycles
                     [(gen/sleep 5)
                      {:type :info
                       :f :rebalance-out
                       :targeter targeter
                       :target-count disrupt-count
                       :target-action :start}
                      (gen/sleep 5)]
                     [{:type :info
                       :f :swap-rebalance
                       :targeter targeter
                       :target-count disrupt-count
                       :target-action :swap}
                      (gen/sleep 5)]
                     []
                     client-generator)

                    :fail-rebalance
                    (do-n-nemesis-cycles
                     cycles
                     [(gen/sleep 5)
                      {:type :info
                       :f :fail-rebalance
                       ;; Nodes don't get removed, so use basic-nodes-targeter
                       :targeter cbnemesis/basic-nodes-targeter
                       :target-count disrupt-count}
                      (gen/sleep 5)]
                     client-generator))))

(defn failover-workload
  "Failover and recover"
  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    recovery-type (opts :recovery-type :delta)
    failover-type (opts :failover-type :hard)
    disrupt-count (opts :disrupt-count 1)
    sg-enabled     (opts :server-groups-enabled)
    server-group-count (if sg-enabled (opts :server-group-count))
    target-server-groups      (if (opts :target-server-groups) (do (assert sg-enabled) true) false)
    random-server-group (if sg-enabled (util/random-server-group server-group-count))
    nemesis   (cbnemesis/couchbase)
    client-generator (client-gen opts)
    generator (do-n-nemesis-cycles cycles
                                   [(gen/sleep 5)
                                    {:type :info
                                     :f    :failover
                                     :failover-type failover-type
                                     :targeter cbnemesis/basic-nodes-targeter
                                     :target-count disrupt-count}
                                    (gen/sleep 10)
                                    {:type :info
                                     :f    :recover
                                     :recovery-type recovery-type}
                                    (gen/sleep 5)]
                                   client-generator)))

(defn kill-workload
  "Register workload that repeatedly kills either memcached, ns_server, or babysitter processes
  while hammering inserts against the cluster"
  [opts]
  (with-register-base opts
    scenario              (opts :scenario)
    replicas              (opts :replicas 1)
    disrupt-count         (opts :disrupt-count 1)
    recovery-type         (opts :recovery-type :delta)
    autofailover          (opts :autofailover false)
    disrupt-count         (opts :disrupt-count 1)
    disrupt-time          (opts :disrupt-time 20)
    sg-enabled     (opts :server-groups-enabled)
    server-group-autofailover (opts :server-group-autofailover false)
    server-group-count (if sg-enabled (opts :server-group-count))
    target-server-groups      (if (opts :target-server-groups) (do (assert sg-enabled) true) false)
    random-server-group (if sg-enabled (util/random-server-group server-group-count))
    complementary-server-group (if sg-enabled (util/complementary-server-group server-group-count random-server-group))
    nemesis               (cbnemesis/couchbase)
    babysitter-targeter   (cbnemesis/start-stop-targeter)
    client-generator      (client-gen opts)
    generator (case scenario
                :kill-memcached
                (do-n-nemesis-cycles cycles
                                     [(gen/sleep 10)
                                      {:type :info
                                       :f :kill-process
                                       :kill-process :memcached
                                       :targeter cbnemesis/basic-nodes-targeter
                                       :target-count disrupt-count}
                                      (gen/sleep 20)]
                                     client-generator)

                :kill-ns-server
                (do-n-nemesis-cycles cycles
                                     [(gen/sleep 10)
                                      {:type :info
                                       :f :kill-process
                                       :kill-process :ns-server
                                       :targeter cbnemesis/basic-nodes-targeter
                                       :target-count disrupt-count}
                                      (gen/sleep 20)]
                                     client-generator)

                :kill-babysitter
                (do-n-nemesis-cycles cycles
                                     [(gen/sleep 5)
                                      {:type :info
                                       :f :kill-process
                                       :kill-process :babysitter
                                       :targeter babysitter-targeter
                                       :target-count disrupt-count
                                       :target-action :start}

                                      (gen/sleep disrupt-time)

                                      {:type :info
                                       :f :start-process
                                       :targeter babysitter-targeter
                                       :target-count disrupt-count
                                       :target-action :stop}

                                      (gen/sleep 5)
                                      {:type :info
                                       :f :recover
                                       :recovery-type recovery-type}
                                      (gen/sleep 5)]
                                     client-generator))))

(defn disk-failure-workload
  "Simulate a disk failure. This workload will not function correctly with docker containers."
  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    disrupt-count (opts :disrupt-count 1)
    disrupt-time   (opts :disrupt-time 10)
    recovery-type  (opts :recovery-type :delta)
    manipulate-disks (do (assert (opts :manipulate-disks) true) (opts :manipulate-disks))
    sg-enabled     (opts :server-groups-enabled)
    server-group-count (if sg-enabled (opts :server-group-count))
    target-server-groups      (if (opts :target-server-groups) (do (assert sg-enabled) true) false)
    autofailover  (opts :autofailover false)
    server-group-autofailover (opts :server-group-autofailover false)
    should-autofailover (and autofailover
                             (>= replicas 1)
                             (= disrupt-count 1)
                             (> disrupt-time autofailover-timeout)
                             (>= node-count 3))
    should-server-group-autofailover (and autofailover
                                          sg-enabled
                                          target-server-groups
                                          server-group-autofailover
                                          (>= replicas 1)
                                          (>= server-group-count 3)
                                          (>= disrupt-count (Math/ceil (/ node-count server-group-count)))
                                          (> disrupt-time autofailover-timeout)
                                          (>= node-count 3))
    random-server-group (if sg-enabled (util/random-server-group server-group-count))
    complementary-server-group (if sg-enabled (util/complementary-server-group server-group-count random-server-group))
    nemesis   (cbnemesis/couchbase)
    client-generator (client-gen opts)

    generator       (do-n-nemesis-cycles
                     cycles
                     [(gen/sleep 10)
                      {:type   :info
                       :f      :fail-disk
                       :targeter-opts {:type      :random-subset
                                       :count     disrupt-count
                                       :condition (merge {:cluster [:active]
                                                          :network [:connected]
                                                          :node [:running]
                                                          :disk [:normal]}
                                                         (when-let [target-sq target-server-groups]
                                                           {:server-group [random-server-group]}))}}

                      (if (or should-autofailover should-server-group-autofailover)
                        [{:type :info
                          :f    :wait-for-autofailover
                          :targeter-opts {:type      :random
                                          :condition (merge {:cluster [:active]
                                                             :network [:connected]
                                                             :node [:running]
                                                             :disk [:normal]}
                                                            (when-let [target-sq target-server-groups]
                                                              {:server-group [complementary-server-group]}))}}
                         (gen/sleep (- disrupt-time autofailover-timeout))]
                        (gen/sleep disrupt-time))

                      {:type   :info
                       :f      :reset-disk
                       :targeter-opts {:type      :all
                                       :condition (merge {:cluster (if (or should-autofailover should-server-group-autofailover)
                                                                     [:failed]
                                                                     [:active])
                                                          :network [:connected]
                                                          :node [:running]
                                                          :disk [:killed]}
                                                         (when-let [target-sq target-server-groups]
                                                           {:server-group [random-server-group]}))}}

                      (if (or should-autofailover should-server-group-autofailover)
                        [(gen/sleep 10)
                         {:type   :info
                          :f      :recover
                          :f-opts {:recovery-type recovery-type}
                          :targeter-opts {:type      :all
                                          :condition {:cluster [:failed]
                                                      :network [:connected]
                                                      :node [:running]
                                                      :disk [:killed]}}}
                         (gen/sleep 5)]
                        [(gen/sleep 10)])]
                     client-generator)))

(defn partition-failover-workload
  "Trigger non-linearizable behaviour where successful mutations with replicate-to=1
  are lost due to promotion of the 'wrong' replica upon failover of the active"
  [opts]
  (with-register-base opts
    replicas       (opts :replicas 1)
    autofailover   (opts :autofailover false)
    server-group-autofailover (opts :server-group-autofailover false)
    disrupt-time   (opts :disrupt-time 20)
    recovery-type (opts :recovery-type :delta)
    failover-type (opts :failover-type :hard)
    disrupt-count  (opts :disrupt-count 1)
    sg-enabled     (opts :server-groups-enabled)
    server-group-count (if sg-enabled (opts :server-group-count))
    target-server-groups      (if (opts :target-server-groups) (do (assert sg-enabled) true) false)
    should-autofailover (and autofailover
                             (>= replicas 1)
                             (= disrupt-count 1)
                             (> disrupt-time autofailover-timeout)
                             (>= node-count 3))
    should-server-group-autofailover (and autofailover
                                          sg-enabled
                                          target-server-groups
                                          server-group-autofailover
                                          (>= replicas 1)
                                          (>= server-group-count 3)
                                          (>= disrupt-count (Math/ceil (/ node-count server-group-count)))
                                          (> disrupt-time autofailover-timeout)
                                          (>= node-count 3))
    random-server-group (if sg-enabled (util/random-server-group server-group-count))
    complementary-server-group (if sg-enabled (util/complementary-server-group server-group-count random-server-group))
    nemesis        (cbnemesis/couchbase)
    client-generator (client-gen opts)
    generator (do-n-nemesis-cycles cycles
                                   [(gen/sleep 5)
                                    {:type   :info
                                     :f      :isolate-completely
                                     :f-opts {:partition-type :isolate-completely}
                                     :targeter cbnemesis/basic-nodes-targeter
                                     :targeter-opts {:type      :random-subset
                                                     :count     disrupt-count
                                                     :condition (merge {:cluster [:active]
                                                                        :network [:connected]
                                                                        :node [:running]}
                                                                       (when-let [target-sq target-server-groups]
                                                                         {:server-group [random-server-group]}))}}
                                    (gen/sleep 5)
                                    {:type :info
                                     :f    :failover
                                     :failover-type failover-type
                                     :targeter cbnemesis/basic-nodes-targeter
                                     :targeter-opts {:type :random-subset
                                                     :count disrupt-count
                                                     :condition (merge {:cluster [:inactive]
                                                                        :network [:partitioned]
                                                                        :node [:running]}
                                                                       (when-let [target-sq target-server-groups]
                                                                         {:server-group [random-server-group]}))}}
                                    (gen/sleep 5)
                                    {:type :info :f :heal-network}
                                    (gen/sleep 10)
                                    {:type :info
                                     :f    :recover
                                     :recovery-type recovery-type
                                     :targeter cbnemesis/basic-nodes-targeter
                                     :targeter-opts {:type :all
                                                     :condition (merge {:cluster [:failed]
                                                                        :network [:connected]
                                                                        :node [:running]}
                                                                       (when-let [target-sq target-server-groups]
                                                                         {:server-group [random-server-group]}))}}
                                    (gen/sleep 5)]  client-generator)))

;; =============
;; Set Workloads
;; =============

(defn Set-workload
  "Generic set workload. We model the set with a bucket, adding an item to the
  set corresponds to inserting a key. To read the set we use a dcp client to
  stream all mutations, keeping track of which keys exist"
  [opts]
  (let-and-merge
   opts
   dcpclient     (if (:dcp-set-read opts)
                   (cbclients/dcp-client))
   cycles        (opts :cycles 1)
   client        (clients/set-client dcpclient)
   concurrency   250
   pool-size     4
   replicas      (opts :replicas 0)
   replicate-to  (opts :replicate-to 0)
   persist-to    (opts :persist-to 0)
   autofailover  (opts :autofailover true)
   autofailover-timeout  (opts :autofailover-timeout 6)
   autofailover-maxcount (opts :autofailover-maxcount 3)

   control-atom  (atom :continue)
   checker       (checker/compose
                  (merge
                   {:timeline (timeline/html)
                    :set (checker/set)
                    :sanity (cbchecker/sanity-check)}
                   (if (opts :perf-graphs)
                     {:perf (checker/perf)})))
   generator     (gen/phases
                  (->> (range)
                       (map (fn [x] {:type :invoke
                                     :f :add
                                     :value x
                                     :replicate-to replicate-to
                                     :persist-to persist-to
                                     :durability-level (util/random-durability-level (:durability opts))}))
                       (gen/seq)
                       (do-n-nemesis-cycles cycles
                                            [(gen/sleep 20)]))
                  (gen/sleep 3)
                  (gen/clients (gen/once {:type :invoke :f :read :value nil})))))

(defn set-kill-workload
  "Set workload that repeatedly kills memcached while hammering inserts against
  the cluster"
  [opts]
  (let-and-merge
   opts
   scenario              (opts :scenario)
   dcpclient             (if (:dcp-set-read opts)
                           (cbclients/dcp-client))
   cycles                (opts :cycles 1)
   concurrency           1000
   pool-size             16
   custom-vbucket-count  (opts :custom-vbucket-count 64)
   replicas              (opts :replicas 1)
   replicate-to          (opts :replicate-to 0)
   persist-to            (opts :persist-to 0)
   disrupt-count         (opts :disrupt-count 1)
   recovery-type         (opts :recovery-type :delta)
   autofailover          (opts :autofailover false)
   autofailover-timeout  (opts :autofailover-timeout  6)
   autofailover-maxcount (opts :autofailover-maxcount 3)
   disrupt-count         (opts :disrupt-count 1)
   disrupt-time          (opts :disrupt-time 30)
   client                (clients/set-client dcpclient)
   nemesis               (cbnemesis/couchbase)
   babysitter-targeter   (cbnemesis/start-stop-targeter)
   control-atom          (atom :continue)
   checker               (checker/compose
                          (merge
                           {:timeline (timeline/html)
                            :set (checker/set)
                            :sanity (cbchecker/sanity-check)}
                           (if (opts :perf-graphs)
                             {:perf (checker/perf)})))
   client-gen (->> (range)
                   (map (fn [x] {:type :invoke
                                 :f :add
                                 :value x
                                 :replicate-to replicate-to
                                 :persist-to persist-to
                                 :durability-level (util/random-durability-level (:durability opts))}))
                   (gen/seq))
   generator  (gen/phases
               (case scenario
                 :kill-memcached-on-slow-disk
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type :info
                                        :f :slow-disk
                                        :targeter cbnemesis/target-all-test-nodes}

                                       (gen/sleep 4)
                                       {:type :info
                                        :f :kill-process
                                        :kill-process :memcached
                                        :targeter cbnemesis/basic-nodes-targeter
                                        :target-count disrupt-count}

                                       {:type :info
                                        :f :reset-disk
                                        :targeter cbnemesis/target-all-test-nodes}

                                       (gen/sleep 10)]
                                      client-gen)

                 :kill-memcached
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type :info
                                        :f :kill-process
                                        :kill-process :memcached
                                        :targeter cbnemesis/basic-nodes-targeter
                                        :target-count disrupt-count}
                                       (gen/sleep 20)]
                                      client-gen)

                 :kill-ns-server
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type :info
                                        :f :kill-process
                                        :kill-process :ns-server
                                        :targeter cbnemesis/basic-nodes-targeter
                                        :target-count disrupt-count}
                                       (gen/sleep 20)]
                                      client-gen)

                 :kill-babysitter
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 5)
                                       {:type :info
                                        :f :kill-process
                                        :kill-process :babysitter
                                        :targeter babysitter-targeter
                                        :target-count disrupt-count
                                        :target-action :start}

                                       (gen/sleep disrupt-time)

                                       {:type :info
                                        :f :start-process
                                        :targeter babysitter-targeter
                                        :target-count disrupt-count
                                        :target-action :stop}

                                       (gen/sleep 5)
                                       {:type :info
                                        :f :recover
                                        :recovery-type recovery-type}
                                       (gen/sleep 10)]
                                      client-gen))
               (gen/clients (gen/once {:type :invoke :f :read :value nil})))))

(defn WhiteRabbit-workload
  "Trigger lost inserts due to one of several white-rabbit variants"
  [opts]
  (let-and-merge
   opts
   cycles                (opts :cycles 5)
   concurrency           500
   pool-size             6
   custom-vbucket-count  (opts :custom-vbucket-count 64)
   replicas              (opts :replicas 0)
   replicate-to          (opts :replicate-to 0)
   persist-to            (opts :persist-to 0)
   autofailover          (opts :autofailover true)
   autofailover-timeout  (opts :autofailover-timeout 6)
   autofailover-maxcount (opts :autofailover-maxcount 3)
   disrupt-count         (opts :disrupt-count 1)
   dcpclient             (if (:dcp-set-read opts)
                           (cbclients/dcp-client))
   client                (clients/set-client dcpclient)

   nemesis               (cbnemesis/couchbase)
   targeter              (cbnemesis/start-stop-targeter)
   control-atom          (atom :continue)
   checker               (checker/compose
                          (merge
                           {:timeline (timeline/html)
                            :set (checker/set)
                            :sanity (cbchecker/sanity-check)}
                           (if (opts :perf-graphs)
                             {:perf (checker/perf)})))
   generator             (gen/phases
                          (->> (range)
                               (map (fn [x]
                                      {:type :invoke
                                       :f :add
                                       :value x
                                       :replicate-to replicate-to
                                       :persist-to persist-to
                                       :durability-level (util/random-durability-level (:durability opts))}))
                               (gen/seq)
                               (do-n-nemesis-cycles cycles
                                                    [(gen/sleep 5)
                                                     {:type :info
                                                      :f :rebalance-out
                                                      :targeter targeter
                                                      :target-count disrupt-count
                                                      :target-action :start}
                                                     (gen/sleep 5)
                                                     {:type :info
                                                      :f :rebalance-in
                                                      :targeter targeter
                                                      :target-count disrupt-count
                                                      :target-action :stop}
                                                     (gen/sleep 5)]))
                          (gen/sleep 3)
                          (gen/clients (gen/once {:type :invoke :f :read :value nil})))))

(defn MB29369-workload
  "Workload to trigger lost inserts due to cursor dropping bug MB29369"
  [opts]
  (let-and-merge
   opts
   ;; Around 100 Kops per node should be sufficient to trigger cursor dropping with
   ;; 100 MB per node bucket quota and ep_cursor_dropping_upper_mark reduced to 30%.
   ;; Since we need the first 2/3 of the ops to cause cursor dropping, we need 150 K
   ;; per node
   oplimit       (opts :oplimit (* (count (opts :nodes)) 150000))
   custom-cursor-drop-marks [20 30]
   concurrency   250
   pool-size     8
   replicas      (opts :replicas 0)
   replicate-to  (opts :replicate-to 0)
   persist-to    (opts :persist-to 0)
   autofailover  (opts :autofailover true)
   autofailover-timeout (opts :autofailover-timeout 6)
   autofailover-maxcount (opts :autofailover-maxcount 3)

   dcpclient     (if (:dcp-set-read opts)
                   (cbclients/dcp-client)
                   (throw (RuntimeException.
                           "MB29369 workload must be used with --dcp-set-read option")))
   client        (clients/set-client dcpclient)
   nemesis       (cbnemesis/couchbase)
   checker       (checker/compose
                  (merge
                   {:timeline (timeline/html)
                    :set (checker/set)}
                   (if (opts :perf-graphs)
                     {:perf (checker/perf)})))

   generator     (gen/phases
                     ;; Start dcp streaming
                  (gen/clients (gen/once {:type :info :f :dcp-start-streaming}))
                     ;; Make DCP slow and write 2/3 of the ops; oplimit should be chosen
                     ;; such that this is sufficient to trigger cursor dropping.
                     ;; ToDo: It would be better if we could monitor ep_cursors_dropped
                     ;;       to ensure that we really have dropped cursors.
                  (->> (range 0 (int (* 2/3 oplimit)))
                       (map (fn [x] {:type :invoke
                                     :f :add
                                     :replicate-to replicate-to
                                     :persist-to persist-to
                                     :durability (util/random-durability-level (:durability opts))
                                     :value x}))
                       (gen/seq)
                       (gen/nemesis (gen/once {:type :info :f :slow-dcp-client})))
                     ;; Make DCP fast again
                  (gen/nemesis (gen/once {:type :info :f :reset-dcp-client}))
                     ;; The race condition causing lost mutations occurs when we stop
                     ;; backfilling, so the dcp stream needs catch up. We pause
                     ;; sending new mutations until we have caught up with half the
                     ;; existing ones in order to speed this up
                  (gen/once
                   (fn []
                     (while (< (count @(:store dcpclient)) (* 1/3 oplimit))
                       (Thread/sleep 5))
                     nil))
                     ;; Write the remainder of the ops.
                  (->> (range (int (* 2/3 oplimit)) oplimit)
                       (map (fn [x] {:type :invoke
                                     :f :add
                                     :replicate-to replicate-to
                                     :persist-to persist-to
                                     :durability (util/random-durability-level (:durability opts))
                                     :value x}))
                       (gen/seq)
                       (gen/clients))
                  (gen/once
                   (fn [] (info "DCP Mutations received thus far:"
                                (count @(:store dcpclient)))))
                     ;; Wait for the cluster to settle before issuing final read
                  (gen/sleep 3)
                  (gen/clients (gen/once {:type :invoke :f :read :value :nil})))))

(defn MB29480-workload
  "Workload to trigger lost deletes due to cursor dropping bug MB29480"
  [opts]
  (let-and-merge
   opts
   dcpclient     (if (:dcp-set-read opts)
                   (cbclients/dcp-client)
                   (throw (RuntimeException.
                           "MB29480 workload must be used with --dcp-set-read option")))
   client        (clients/set-client dcpclient)

      ;; Around 100 Kops per node should be sufficient to trigger cursor dropping with
      ;; 100 MB per node bucket quota and ep_cursor_dropping_upper_mark reduced to 30%.
   oplimit       (opts :oplimit (+ (* (count (opts :nodes)) 100000) 50000))
   custom-cursor-drop-marks [20 30]
   concurrency   250
   pool-size     4
   replicas      (opts :replicas 0)
   replicate-to  (opts :replicate-to 0)
   persist-to    (opts :persist-to 0)
   autofailover  (opts :autofailover true)
   autofailover-timeout (opts :autofailover-timeout 6)
   autofailover-maxcount (opts :autofailover-maxcount 3)
   nemesis       (nemesis/compose {#{:slow-dcp-client
                                     :reset-dcp-client
                                     :trigger-compaction} (cbnemesis/couchbase)
                                   {:bump-time :bump}     (nt/clock-nemesis)})
   checker       (checker/compose
                  (merge
                   {:timeline (timeline/html)
                    :set (cbchecker/extended-set-checker)}
                   (if (opts :perf-graphs)
                     {:perf (checker/perf)})))

   generator     (gen/phases
                     ;; Start dcp stream
                  (gen/clients (gen/once {:type :info :f :dcp-start-streaming}))
                     ;; First create 10000 keys and let the client see them
                  (->> (range 0 10000)
                       (map (fn [x] {:type :invoke
                                     :f :add
                                     :replicate-to replicate-to
                                     :persist-to persist-to
                                     :durability (util/random-durability-level (:durability opts))
                                     :value x}))
                       (gen/seq)
                       (gen/clients))
                     ;; Then slow down dcp and make sure the queue is filled
                  (->> (range 10000 50000)
                       (map (fn [x] {:type :invoke
                                     :f :add
                                     :replicate-to replicate-to
                                     :persist-to persist-to
                                     :durability (util/random-durability-level (:durability opts))
                                     :value x}))
                       (gen/seq)
                       (gen/nemesis (gen/once {:type :info :f :slow-dcp-client})))
                     ;; Now delete the keys
                  (->> (range 0 50000)
                       (map (fn [x] {:type :invoke
                                     :f :del
                                     :replicate-to replicate-to
                                     :persist-to persist-to
                                     :durability (util/random-durability-level (:durability opts))
                                     :value x}))
                       (gen/seq)
                       (gen/clients))
                     ;; Now trigger tombstone purging. We need to bump time by at least 3
                     ;; days to allow metadata to be purged
                  (gen/nemesis (gen/seq [{:type :info :f :bump-time
                                          :value (zipmap (opts :nodes) (repeat 260000000))}
                                         (gen/sleep 1)
                                         {:type :info :f :trigger-compaction}]))
                     ;; Wait some time to allow compaction to run
                  (gen/sleep 20)
                     ;; Ensure cursor dropping by spamming inserts while dcp is still slow
                  (->> (range 50000 oplimit)
                       (map (fn [x] {:type :invoke
                                     :f :add
                                     :replicate-to replicate-to
                                     :persist-to persist-to
                                     :durability (util/random-durability-level (:durability opts))
                                     :value x}))
                       (gen/seq)
                       (gen/clients))

                     ;; Final read
                  (gen/sleep 3)
                  (gen/clients (gen/once {:type :invoke :f :read :value nil})))))
