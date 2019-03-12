(ns couchbase.workload
  (:require [clojure.tools.logging :refer :all]
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

;; =============
;; Helper macros
;; =============

(defmacro let-and-merge
  "Take pairs of parameter-name parameter-value and merge these into a map. Bind
  all previous parameter names to their value to allow parameters values
  dependent on previous values."
  ([] {})
  ([param value & more] `(merge {(keyword (name '~param)) ~value}
                                (let [~param ~value]
                                  (let-and-merge ~@more)))))

(defmacro with-register-base
  "Apply a set of shared parameters used across the register workloads before
  merging the custom parameters"
  ([opts & more]
   `(let-and-merge
        ~'cycles                (~opts :cycles 1)
        ~'node-count            (~opts :node-count (count (~opts :nodes)))
        ~'nodes                 (if (>= (count (~opts :nodes)) ~'node-count)
                                  (vec (take ~'node-count (~opts :nodes)))
                                  (assert false "node-count greater than available nodes"))
        ~'server-group-count    (~opts :server-group-count 1)
        ~'rate                  (~opts :rate 1/3)
        ~'doc-count             (~opts :doc-count 40)
        ~'doc-threads           (~opts :doc-threads 3)
        ~'concurrency           (* ~'doc-count ~'doc-threads)
        ~'pool-size             (~opts :pool-size 6)
        ~'autofailover-timeout  (~opts :autofailover-timeout 6)
        ~'autofailover-maxcount (~opts :autofailover-maxcount 3)
        ~'client                (clients/register-client)
        ~'model                 (model/cas-register :nil)
        ~'checker   (checker/compose
                     (merge
                      {:indep (independent/checker
                               (checker/compose
                                {:timeline (timeline/html)
                                 :linear (checker/linearizable)}))}
                      (if (~opts :perf-graphs)
                        {:perf (checker/perf)})))
        ~@more)))

;; =================
;; HELPER GENERATORS
;; =================

(gen/defgenerator while-atom-true
  [controlatom gen]
  [controlatom gen]
  (op [_ test process]
      (if @controlatom
        (gen/op gen test process))))


(gen/defgenerator set-atom-false
  [controlatom]
  [controlatom]
  (op [_ test process]
      (reset! controlatom false)
      nil))

(defn do-n-nemesis-cycles
  ([n nemesis-seq client-gen] (do-n-nemesis-cycles n [] nemesis-seq [] client-gen))
  ([n nemesis-pre-seq nemesis-seq nemesis-post-seq client-gen]
   (let [control-atom (atom true)
         nemesis-gen  (gen/seq (flatten [nemesis-pre-seq
                                         (repeat n nemesis-seq)
                                         nemesis-post-seq
                                         (gen/sleep 3)
                                         (set-atom-false. control-atom)]))
         rclient-gen  (while-atom-true. control-atom client-gen)]
     (gen/nemesis nemesis-gen rclient-gen))))

;; ==================
;; Register workloads
;; ==================

(defn register-workload
  "Basic register workload"
  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    replicate-to  (opts :replicate-to 0)
    nemesis       nemesis/noop
    generator     (->> (independent/concurrent-generator doc-threads (range)
                         (fn [k]
                           (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                          (fn [_ _] {:type :invoke
                                                     :f :write
                                                     :value (rand-int 5)
                                                     :durability-level (util/random-durability-level opts)})
                                          (fn [_ _] {:type :invoke
                                                     :f :cas
                                                     :value [(rand-int 5) (rand-int 5)]
                                                     :durability-level (util/random-durability-level opts)})])
                                (gen/stagger (/ rate)))))
                       (do-n-nemesis-cycles cycles
                                            [(gen/sleep 20)]))))

(defn partition-workload
  "Paritions the network by isolating nodes from each other, then will recover if autofailover happens"
  [opts]
  (with-register-base opts
    replicas       (opts :replicas 1)
    replicate-to   (opts :replicate-to 0)
    autofailover   (opts :autofailover false)
    server-group-autofailover (opts :server-group-autofailover false))
    disrupt-time   (opts :disrupt-time 20)
    recovery-type  (opts :recovery-type :delta)
    disrupt-count  (opts :disrupt-count 1)
    should-autofailover (and autofailover (>= replicas 1) (= disrupt-count 1)
                             (> disrupt-time autofailover-timeout) (>= node-count 3))
    should-server-group-autofailover (and autofailover server-group-autofailover (>= replicas 1) (>= (:server-group-count opts) 3)
                                          (>= disrupt-count (int (Math/ceil (/ node-count (:server-group-count opts)))))
                                          (> disrupt-time autofailover-timeout) (>= node-count 3))
    nemesis        (cbnemesis/couchbase)
    client-generator (independent/concurrent-generator
                       doc-threads (range)
                       (fn [k]
                         (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                        (fn [_ _] {:type :invoke
                                                   :f :write
                                                   :value (rand-int 50)
                                                   :durability-level (util/random-durability-level opts)})])
                              (gen/stagger (/ rate)))))

    generator       (do-n-nemesis-cycles
                      cycles
                      [(gen/sleep 5)
                       {:type   :info
                        :f      :partition-network
                        :f-opts {:partition-type :isolate-completely}
                        :targeter-opts {:type      :random-subset
                                        :count     disrupt-count
                                        :condition {:cluster [:active]
                                                    :network [:connected]
                                                    :node [:running]
                                                    :server-group ["Group 1"]}}}

                       (if (or should-autofailover should-server-group-autofailover)
                         [{:type :info
                           :f    :wait-for-autofailover
                           :targeter-opts {:type      :random
                                           :condition {:cluster [:active]
                                                       :network [:connected]
                                                       :node [:running]}}}
                          (gen/sleep (- disrupt-time autofailover-timeout))]
                         (gen/sleep disrupt-time))

                       {:type :info :f :heal-network}

                       (if (or should-autofailover should-server-group-autofailover)
                         [(gen/sleep (if should-server-group-autofailover 20 10))
                          {:type   :info
                           :f      :recover
                           :f-opts {:recovery-type recovery-type}
                           :targeter-opts {:type      :all
                                           :condition {:cluster [:failed]
                                                       :network [:connected]
                                                       :node [:running]}}}
                          (gen/sleep 5)]
                         [])]
                      client-generator)))

(defn Partition-Failover-workload
  "Trigger non-linearizable behaviour where successful mutations with replicate-to=1
  are lost due to promotion of the 'wrong' replica upon failover of the active"
  [opts]
  (with-register-base opts
    replicas     (opts :replicas 2)
    replicate-to (opts :replicate-to 1)
    autofailover (opts :autofailover false)
    nemesis      (cbnemesis/partition-then-failover)
    generator    (->> (independent/concurrent-generator doc-threads (range)
                       (fn [k]
                         (->> (gen/mix [(fn [_ _] {:type :invoke, :f :read, :value nil})
                                        (fn [_ _] {:type :invoke
                                                   :f :write
                                                   :value (rand-int 50)
                                                   :durability-level (util/random-durability-level opts)})])
                              (gen/limit (* 50 (+ 0.5 (rand))))
                              (gen/stagger (/ rate)))))
                      (do-n-nemesis-cycles cycles
                                           [(gen/sleep 10)
                                            {:type :info :f :start-partition}
                                            (gen/sleep 5)
                                            {:type :info :f :start-failover}
                                            (gen/sleep 10)
                                            {:type :info :f :stop-partition}
                                            (gen/sleep 5)
                                            {:type :info :f :recover}
                                            (gen/sleep 5)]))))

(defn Rebalance-workload
  "Rebalance scenarios:
  Sequential Rebalance In/Out - rebalance out a single node at a time until disrupt-count nodes have been
                                rebalanced out. This is followed by the opposite - rebalance back in a single
                                node at a time until disrupt-count nodes have been rebalanced back in.

  Bulk Rebalance In/Out       - rebalance out disrupt-count nodes at once followed by rebalancing in the same
                                node at once.

  Swap Rebalance              - first disrupt-count nodes are rebalanced out of the cluster. Then, these nodes
                                are used to perform a swap rebalance with dirsupt-count nodes that are still
                                in the cluster"

  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    replicate-to  (opts :replicate-to 0)
    scenario      (opts :scenario)
    disrupt-count (if (= scenario :swap-rebalance)
                    (let [disrupt-count (opts :disrupt-count 1)
                          max-disrupt-count (- (Math/ceil (double (/ (count (:nodes opts)) 2))) 1)]
                      (assert (<= disrupt-count max-disrupt-count))
                      disrupt-count)
                    (let [disrupt-count (opts :disrupt-count 1)
                          max-disrupt-count (- (count (:nodes opts)) 1)]
                      (assert (<= disrupt-count max-disrupt-count))
                      disrupt-count))
    autofailover  (opts :autofailover false)
    nemesis       (cbnemesis/couchbase)
    client-generator (independent/concurrent-generator
                       doc-threads (range)
                       (fn [k]
                         (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                        (fn [_ _] {:type :invoke
                                                   :f :write
                                                   :value (rand-int 50)
                                                   :durability-level (util/random-durability-level opts)})])
                              (gen/stagger (/ rate)))))
    generator     (case scenario
                    :sequential-rebalance-out-in
                    (do-n-nemesis-cycles
                      cycles
                      [(repeatedly
                         disrupt-count
                         #(do [(gen/sleep 5)
                               {:type :info :f :rebalance-out
                                :targeter-opts
                                      {:type :random
                                       :condition {:cluster [:active :failed]
                                                   :network [:connected]
                                                   :node [:running]}}}]))
                       (gen/sleep 5)
                       (repeatedly
                         disrupt-count
                         #(do [(gen/sleep 5)
                               {:type :info :f :rebalance-in
                                :targeter-opts
                                      {:type :random
                                       :condition {:cluster [:ejected]
                                                   :network [:connected]
                                                   :node [:running]}}}]))
                       (gen/sleep 5)]
                      client-generator)

                    :bulk-rebalance-out-in
                    (do-n-nemesis-cycles
                      cycles
                      [(gen/sleep 5)
                       {:type :info :f :rebalance-out
                        :targeter-opts
                              {:type :random-subset
                               :count disrupt-count
                               :condition {:cluster [:active :failed]
                                           :network [:connected]
                                           :node [:running]}}}
                       (gen/sleep 5)
                       {:type :info :f :rebalance-in
                        :targeter-opts
                              {:type :random-subset
                               :count disrupt-count
                               :condition {:cluster [:ejected]
                                           :network [:connected]
                                           :node [:running]}}}
                       (gen/sleep 5)] client-generator)

                    :swap-rebalance
                    (do-n-nemesis-cycles
                      cycles
                      [(gen/sleep 5)
                       {:type :info :f :rebalance-out
                        :targeter-opts
                              {:type :random-subset
                               :count disrupt-count
                               :condition {:cluster [:active :inactive :failed]
                                           :network [:connected]
                                           :node [:running]}
                               }}
                       (gen/sleep 5)]
                      [{:type :info :f :swap-rebalance
                        :targeter-opts
                              {:type :random-subset
                               :count disrupt-count
                               :condition {:cluster [:ejected]
                                           :network [:connected]
                                           :node [:running]}}}
                       (gen/sleep 5)]
                      [] client-generator)

                    )))

(defn Fail-Rebalance-workload
  "Kill memcached during a rebalance"
  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    replicate-to  (opts :replicate-to 0)
    disrupt-count (opts :disrupt-count 1)
    autofailover  (opts :autofailover false)
    nemesis       (cbnemesis/fail-rebalance)
    generator     (->> (independent/concurrent-generator doc-threads (range)
                         (fn [k]
                           (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                          (fn [_ _] {:type :invoke
                                                     :f :write
                                                     :value (rand-int 50)
                                                     :durability-level (util/random-durability-level opts)})])
                                (gen/stagger (/ rate)))))
                       (do-n-nemesis-cycles cycles
                                            [(gen/sleep 5)
                                             {:type :info :f :start :count disrupt-count}
                                             (gen/sleep 5)]))))

(defn Failover-workload
  "Failover and recover"
  [opts]
  (with-register-base opts
    replicas      (opts :replicas 1)
    replicate-to  (opts :replicate-to 0)
    recovery-type (opts :recovery-type :delta)
    failover-type (opts :failover-type :hard)
    disrupt-count (opts :disrupt-count 1)
    nemesis      (cbnemesis/couchbase)
    generator    (->> (independent/concurrent-generator doc-threads (range)
                        (fn [k]
                          (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                         (fn [_ _] {:type :invoke
                                                    :f :write
                                                    :value (rand-int 50)
                                                    :durability-level (util/random-durability-level opts)})])
                               (gen/stagger (/ rate)))))
                      (do-n-nemesis-cycles cycles
                                           [(gen/sleep 5)
                                            {:type :info
                                             :f    :failover
                                             :f-opts {:failover-type failover-type}
                                             :targeter-opts {:type :random-subset
                                                             :count disrupt-count
                                                             :condition {:cluster [:active :inactive]
                                                                         :network [:connected]}}}
                                            (gen/sleep 10)
                                            {:type :info
                                             :f    :recover
                                             :f-opts {:recovery-type recovery-type}
                                             :targeter-opts {:type :all
                                                             :condition {:cluster [:failed]
                                                                         :network [:connected]
                                                                         :node [:running]}}}
                                            (gen/sleep 5)]))))

(defn Disk-Failure-workload
  "Simulate a disk failure. This workload will not function correctly with docker containers."
  [opts]
  (with-register-base opts
    replicas      (opts :replicas 2)
    replicate-to  (opts :replicate-to 0)
    disrupt-count (opts :disrupt-count 1)
    autofailover  (opts :autofailover false)
    recovery      (opts :recovery :delta)
    nemesis       (cbnemesis/disk-failure)
    generator     (->> (independent/concurrent-generator doc-threads (range)
                         (fn [k]
                           (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                          (fn [_ _] {:type :invoke
                                                     :f :write
                                                     :value (rand-int 50)
                                                     :durability-level (util/random-durability-level opts)})])
                                (gen/limit 20)
                                (gen/stagger (/ rate)))))
                       (gen/nemesis (gen/seq [(gen/sleep 15)
                                              {:type :info :f :start :count disrupt-count}]))
                       (gen/limit 1500))))

;; =============
;; Set Workloads
;; =============

(defn Set-workload
  "Generic set workload. We model the set with a bucket, adding an item to the
  set corresponds to inserting a key. To read the set we use a dcp client to
  stream all mutations, keeping track of which keys exist"
  [opts]
  (let-and-merge
      dcpclient     (cbclients/dcp-client)

      cycles        (opts :cycles 1)
      client        (clients/set-client dcpclient)
      concurrency   250
      pool-size     4
      replicas      (opts :replicas 0)
      replicate-to  (opts :replicate-to 0)
      autofailover  (opts :autofailover true)
      autofailover-timeout  (opts :autofailover-timeout 6)
      autofailover-maxcount (opts :autofailover-maxcount 3)

      checker       (checker/compose
                      (merge
                        {:set (checker/set)}
                        (if (opts :perf-graphs)
                          {:perf (checker/perf)})))
      generator     (gen/phases
                       (->> (range)
                            (map (fn [x] {:type :invoke
                                          :f :add
                                          :value x
                                          :durability-level (util/random-durability-level opts)}))
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
      scenario              (opts :scenario)
      dcpclient             (cbclients/dcp-client)
      cycles                (opts :cycles 1)
      concurrency           1000
      pool-size             16
      custom-vbucket-count  64
      replicas              (opts :replicas 1)
      replicate-to          (opts :replicate-to 0)
      disrupt-count         (opts :disrupt-count 1)
      recovery-type         (opts :recovery-type :delta)
      autofailover          (opts :autofailover false)
      autofailover-timeout  (opts :autofailover-timeout  6)
      autofailover-maxcount (opts :autofailover-maxcount 3)
      disrupt-count         (opts :disrupt-count 1)
      disrupt-time          (opts :disrupt-time 30)
      should-autofailover   (and autofailover (= disrupt-count 1) (> disrupt-time autofailover-timeout) (>= (count (:nodes opts)) 3))
      client                (clients/set-client dcpclient)
      nemesis               (cbnemesis/couchbase)
      checker               (checker/compose
                             (merge
                              {:set (checker/set)}
                              (if (opts :perf-graphs)
                                {:perf (checker/perf)})))
      client-gen (->> (range)
                      (map (fn [x] {:type :invoke
                                    :f :add
                                    :value x
                                    :durability-level (util/random-durability-level opts)}))
                      (gen/seq))
      generator             (gen/phases
                              (case scenario
                                :kill-memcached
                                (do-n-nemesis-cycles cycles
                                                     [(gen/sleep 10)
                                                      {:type :info
                                                       :f    :kill-process
                                                       :f-opts {:process :memcached}
                                                       :targeter-opts {:type :random-subset
                                                                       :count disrupt-count
                                                                       :condition {:cluster [:active]
                                                                                   :network [:connected]
                                                                                   :node [:running]}}}
                                                      (gen/sleep 20)] client-gen)

                                :kill-ns-server
                                (do-n-nemesis-cycles cycles
                                                     [(gen/sleep 10)
                                                      {:type :info
                                                       :f    :kill-process
                                                       :f-opts {:process :ns-server}
                                                       :targeter-opts {:type :random-subset
                                                                       :count disrupt-count
                                                                       :condition {:cluster [:active]
                                                                                   :network [:connected]
                                                                                   :node [:running]}}}
                                                      (gen/sleep 20)] client-gen)

                                :kill-babysitter
                                (do-n-nemesis-cycles cycles
                                                     [(gen/sleep 5)
                                                      {:type :info
                                                       :f    :kill-process
                                                       :f-opts {:process :babysitter}
                                                       :targeter-opts {:type :random-subset
                                                                       :count disrupt-count
                                                                       :condition {:cluster [:active]
                                                                                   :network [:connected]
                                                                                   :node [:running]}}}
                                                      (if should-autofailover
                                                        [{:type :info
                                                          :f    :wait-for-autofailover
                                                          :targeter-opts {:type      :random
                                                                          :condition {:cluster [:active]
                                                                                      :network [:connected]
                                                                                      :node [:running]}}}
                                                         (gen/sleep (- disrupt-time autofailover-timeout))]
                                                        (gen/sleep disrupt-time))

                                                      {:type :info
                                                       :f    :start-process
                                                       :f-opts {:process :couchbase-server}
                                                       :targeter-opts {:type      :all
                                                                       :condition {:cluster [:inactive :failed]
                                                                                   :network [:connected]
                                                                                   :node [:killed]}}}
                                                      (gen/sleep 5)
                                                      (if should-autofailover
                                                        [{:type   :info
                                                          :f      :recover
                                                          :f-opts {:recovery-type recovery-type}
                                                          :targeter-opts {:type      :all
                                                                          :condition {:cluster [:failed]
                                                                                      :network [:connected]
                                                                                      :node [:running]}}}
                                                         (gen/sleep 5)]
                                                        [])
                                                      ] client-gen)
                                )
                             (gen/clients (gen/once {:type :invoke :f :read :value nil})))


      ))

(defn WhiteRabbit-workload
  "Trigger lost inserts due to one of several white-rabbit variants"
  [opts]
  (let-and-merge
      cycles                (opts :cycles 5)
      concurrency           500
      pool-size             6
      custom-vbucket-count  64
      replicas              (opts :replicas 0)
      replicate-to          (opts :replicate-to 0)
      autofailover          (opts :autofailover true)
      autofailover-timeout  (opts :autofailover-timeout 6)
      autofailover-maxcount (opts :autofailover-maxcount 3)
      disrupt-count         (opts :disrupt-count 1)
      dcpclient             (cbclients/dcp-client)
      client                (clients/set-client dcpclient)

      nemesis               (cbnemesis/couchbase)
      checker               (checker/compose
                             (merge
                              {:set (checker/set)}
                              (if (opts :perf-graphs)
                                {:perf (checker/perf)})))
      generator             (gen/phases
                             (->> (range)
                                  (map (fn [x] {:type :invoke
                                                :f :add
                                                :value x
                                                :durability-level (util/random-durability-level opts)}))
                                  (gen/seq)
                                  (do-n-nemesis-cycles cycles
                                                       [(gen/sleep 5)
                                                        {:type :info :f :rebalance-out
                                                         :targeter-opts
                                                               {:type :random-subset
                                                                :count disrupt-count
                                                                :condition [:active :failed]}}
                                                        (gen/sleep 5)
                                                        {:type :info :f :rebalance-in
                                                         :targeter-opts
                                                               {:type :random-subset
                                                                :count disrupt-count
                                                                :condition [:ejected]}}
                                                        (gen/sleep 5)]))
                             (gen/sleep 3)
                             (gen/clients (gen/once {:type :invoke :f :read :value nil})))))

(defn MB29369-workload
  "Workload to trigger lost inserts due to cursor dropping bug MB29369"
  [opts]
  (let-and-merge
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
      autofailover  (opts :autofailover true)
      autofailover-timeout     (opts :autofailover-timeout 6)
      autofailover-maxcount    (opts :autofailover-maxcount 3)

      dcpclient     (cbclients/dcp-client)
      client        (clients/set-client dcpclient)
      nemesis       (cbnemesis/couchbase)
      checker       (checker/compose
                     (merge
                      {:set (checker/set)}
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
                          (map (fn [x] {:type :invoke :f :add :value x}))
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
                          (map (fn [x] {:type :invoke :f :add :value x}))
                          (gen/seq)
                          (gen/clients))
                     (gen/once
                      (fn [] (info "DCP Mutations received thus far:" (count @(:store dcpclient))) nil))
                     ;; Wait for the cluster to settle before issuing final read
                     (gen/sleep 3)
                     (gen/clients (gen/once {:type :invoke :f :read :value :nil})))))

(defn MB29480-workload
  "Workload to trigger lost deletes due to cursor dropping bug MB29480"
  [opts]
  (let-and-merge
      dcpclient     (cbclients/dcp-client)
      client        (clients/set-client dcpclient)

      ;; Around 100 Kops per node should be sufficient to trigger cursor dropping with
      ;; 100 MB per node bucket quota and ep_cursor_dropping_upper_mark reduced to 30%.
      oplimit       (opts :oplimit (+ (* (count (opts :nodes)) 100000) 50000))
      custom-cursor-drop-marks [20 30]
      concurrency   250
      pool-size     4
      replicas      (opts :replicas 0)
      replicate-to  (opts :replicate-to 0)
      autofailover  (opts :autofailover true)
      autofailover-timeout (opts :autofailover-timeout 6)
      autofailover-maxcount (opts :autofailover-maxcount 3)
      nemesis       (nemesis/compose {{:slow-dcp-client :slow-dcp-client
                                       :reset-dcp-client :reset-dcp-client
                                       :trigger-compaction :trigger-compaction} (cbnemesis/couchbase)
                                      {:bump-time :bump}          (nt/clock-nemesis)})
      checker       (checker/compose
                     (merge
                      {:set (cbchecker/extended-set-checker)}
                      (if (opts :perf-graphs)
                        {:perf (checker/perf)})))

      generator     (gen/phases
                     ;; Start dcp stream
                     (gen/clients (gen/once {:type :info :f :dcp-start-streaming}))
                     ;; First create 10000 keys and let the client see them
                     (->> (range 0 10000)
                          (map (fn [x] {:type :invoke :f :add :value x}))
                          (gen/seq)
                          (gen/clients))
                     ;; Then slow down dcp and make sure the queue is filled
                     (->> (range 10000 50000)
                          (map (fn [x] {:type :invoke :f :add :value x}))
                          (gen/seq)
                          (gen/nemesis (gen/once {:type :info :f :slow-dcp-client})))
                     ;; Now delete the keys
                     (->> (range 0 50000)
                          (map (fn [x] {:type :invoke :f :del :value x}))
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
                          (map (fn [x] {:type :invoke :f :add :value x}))
                          (gen/seq)
                          (gen/clients))

                     ;; Final read
                     (gen/sleep 3)
                     (gen/clients (gen/once {:type :invoke :f :read :value nil})))))
