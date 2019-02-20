(ns couchbase.workload
  (:require [clojure.tools.logging :refer :all]
            [couchbase [checker   :as cbchecker]
                       [clients   :as clients]
                       [cbclients :as cbclients]
                       [nemesis   :as cbnemesis]]
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
        ~'cycles                (or (~opts :cycles)      1)
        ~'rate                  (or (~opts :rate)        1/3)
        ~'doc-count             (or (~opts :doc-count)   40)
        ~'doc-threads           (or (~opts :doc-threads) 3)
        ~'concurrency           (* ~'doc-count ~'doc-threads)
        ~'autofailover-timeout  (or (~opts :autofailover-timeout)  6)
        ~'autofailover-maxcount (or (~opts :autofailover-maxcount) 3)
        ~'client                (clients/register-client (cbclients/basic-client))
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
     (gen/nemesis nemesis-gen rclient-gen)))
  )

;; ==================
;; Register workloads
;; ==================

(defn Register-workload
  "Basic register workload"
  [opts]
  (with-register-base opts
    replicas      (or (opts :replicas) 1)
    replicate-to  (or (opts :replicate-to) 0)
    nemesis       nemesis/noop
    generator     (->> (independent/concurrent-generator doc-threads (range)
                         (fn [k]
                           (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                          (fn [_ _] {:type :invoke :f :write :value (rand-int 5)})
                                          (fn [_ _] {:type :invoke :f :cas   :value [(rand-int 5) (rand-int 5)]})])
                                (gen/stagger (/ rate)))))
                       (do-n-nemesis-cycles cycles
                                            [(gen/sleep 20)]))))

(defn Partition-workload
  "Paritions the network by isolating nodes from each other, then will recover if autofailover happens"
  [opts]
  (with-register-base opts
    replicas       (or (opts :replicas) 1)
    replicate-to   (or (opts :replicate-to) 0)
    autofailover   (if (nil? (opts :autofailover)) false (opts :autofailover))
    disrupt-time   (or (opts :disrupt-time) 20)
    recovery-type  (or (keyword (opts :recovery-type)) :delta)
    disrupt-count  (or (opts :disrupt-count) 1)
    should-autofailover (and autofailover (= disrupt-count 1) (> disrupt-time autofailover-timeout) (>= (count (:nodes opts)) 3))
    nemesis        (cbnemesis/couchbase)
    client-generator (independent/concurrent-generator
                       doc-threads (range)
                       (fn [k]
                         (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                        (fn [_ _] {:type :invoke :f :write :value (rand-int 50)})])
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

                       {:type :info :f :heal-network}
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
                         [])]
                      client-generator)))

(defn Partition-Failover-workload
  "Trigger non-linearizable behaviour where successful mutations with replicate-to=1
  are lost due to promotion of the 'wrong' replica upon failover of the active"
  [opts]
  (with-register-base opts
    replicas     (or (opts :replicas) 2)
    replicate-to (or (opts :replicate-to) 1)
    autofailover (if (nil? (opts :autofailover)) false (opts :autofailover))
    nemesis      (cbnemesis/partition-then-failover)
    generator    (->> (independent/concurrent-generator doc-threads (range)
                       (fn [k]
                         (->> (gen/mix [(fn [_ _] {:type :invoke, :f :read, :value nil})
                                        (fn [_ _] {:type :invoke, :f :write :value (rand-int 50)})])
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
  "Rebalance a nodes out and back into the cluster sequentially"
  [opts]
  (with-register-base opts
    replicas      (or (opts :replicas)      1)
    replicate-to  (or (opts :replicate-to)  0)
    scenario      (opts :scenario)
    disrupt-count (if (= scenario :swap-rebalance)
                    (let [disrupt-count (or (opts :disrupt-count) 1)
                          max-disrupt-count (- (Math/ceil (double (/ (count (:nodes opts)) 2))) 1)]
                      (assert (<= disrupt-count max-disrupt-count))
                      disrupt-count)
                    (let [disrupt-count (or (opts :disrupt-count) 1)
                          max-disrupt-count (- (count (:nodes opts)) 1)]
                      (assert (<= disrupt-count max-disrupt-count))
                      disrupt-count)
                    )
    autofailover  (if (nil? (opts :autofailover)) false (opts :autofailover))
    nemesis       (cbnemesis/couchbase)
    client-generator (independent/concurrent-generator
                       doc-threads (range)
                       (fn [k]
                         (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                        (fn [_ _] {:type :invoke :f :write :value (rand-int 50)})])
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
    replicas      (or (opts :replicas)      1)
    replicate-to  (or (opts :replicate-to)  0)
    disrupt-count (or (opts :disrupt-count) 1)
    autofailover  (if (nil? (opts :autofailover)) false (opts :autofailover))
    nemesis       (cbnemesis/fail-rebalance)
    generator     (->> (independent/concurrent-generator doc-threads (range)
                         (fn [k]
                           (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                          (fn [_ _] {:type :invoke :f :write :value (rand-int 50)})])
                                (gen/stagger (/ rate)))))
                       (do-n-nemesis-cycles cycles
                                            [(gen/sleep 5)
                                             {:type :info :f :start :count disrupt-count}
                                             (gen/sleep 5)]))))

(defn Failover-workload
  "Failover and recover"
  [opts]
  (with-register-base opts
    replicas      (or (opts :replicas)     1)
    replicate-to  (or (opts :replicate-to) 0)
    recovery-type (or (keyword (opts :recovery-type)) :delta)
    failover-type (or (keyword (opts :failover-type)) :hard)
    disrupt-count (or (opts :disrupt-count) 1)
    nemesis      (cbnemesis/couchbase)
    generator    (->> (independent/concurrent-generator doc-threads (range)
                        (fn [k]
                          (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                         (fn [_ _] {:type :invoke :f :write :value (rand-int 50)})])
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
    replicas      (or (opts :replicas)      2)
    replicate-to  (or (opts :replicate-to)  0)
    disrupt-count (or (opts :disrupt-count) 1)
    autofailover  (if (nil? (opts :autofailover)) false (opts :autofailover))
    recovery      (or (opts :recovery) :delta)
    nemesis       (cbnemesis/disk-failure)
    generator     (->> (independent/concurrent-generator doc-threads (range)
                         (fn [k]
                           (->> (gen/mix [(fn [_ _] {:type :invoke :f :read  :value nil})
                                          (fn [_ _] {:type :invoke :f :write :value (rand-int 50)})])
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
      addclient     (cbclients/batch-insert-pool)
      delclient     nil
      dcpclient     (cbclients/simple-dcp-client)

      cycles        (or (opts :cycles) 1)
      client        (clients/set-client addclient delclient dcpclient)
      concurrency   250
      batch-size    50
      pool-size     4
      replicas      (or (opts :replicas) 0)
      replicate-to  (or (opts :replicate-to) 0)
      autofailover  (if (nil? (opts :autofailover)) true (opts :autofailover))
      autofailover-timeout  (or (opts :autofailover-timeout)  6)
      autofailover-maxcount (or (opts :autofailover-maxcount) 3)

      checker       (checker/compose
                      (merge
                        {:set (checker/set)}
                        (if (opts :perf-graphs)
                          {:perf (checker/perf)})))
      generator     (gen/phases
                       (->> (range)
                            (map (fn [x] {:type :invoke :f :add :value x}))
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
      scenario              (keyword (opts :scenario))
      addclient             (cbclients/batch-insert-pool)
      delclient             nil
      dcpclient             (cbclients/simple-dcp-client)
      cycles                (or (opts :cycles) 1)
      concurrency           (case scenario :kill-memcached 500 :kill-ns-server 500 :kill-babysitter 1000)
      batch-size            50
      pool-size             (case scenario :kill-memcached 6 :kill-ns-server 6 :kill-babysitter 16)
      custom-vbucket-count  64
      replicas              (or (opts :replicas) 1)
      replicate-to          (or (opts :replicate-to) 0)
      disrupt-count         (or (opts :disrupt-count) 1)
      recovery-type         (or (opts :recovery-type) :delta)
      autofailover          (if (nil? (opts :autofailover)) false (opts :autofailover))
      autofailover-timeout  (or (opts :autofailover-timeout)  6)
      autofailover-maxcount (or (opts :autofailover-maxcount) 3)
      disrupt-count         (or (opts :disrupt-count) 1)
      disrupt-time          (or (opts :disrupt-time)  30)
      should-autofailover   (and autofailover (= disrupt-count 1) (> disrupt-time autofailover-timeout) (>= (count (:nodes opts)) 3))
      client                (clients/set-client addclient delclient dcpclient)
      nemesis               (cbnemesis/couchbase)
      checker               (checker/compose
                             (merge
                              {:set (checker/set)}
                              (if (opts :perf-graphs)
                                {:perf (checker/perf)})))
      client-gen (->> (range)
                      (map (fn [x] {:type :invoke :f :add :value x}))
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
      addclient             (cbclients/batch-insert-pool)
      delclient             nil
      dcpclient             (cbclients/simple-dcp-client)
      cycles                (or (opts :cycles) 5)
      concurrency           500
      batch-size            50
      pool-size             6
      custom-vbucket-count  64
      replicas              (or (opts :replicas) 0)
      replicate-to          (or (opts :replicate-to) 0)
      autofailover          (if (nil? (opts :autofailover)) true (opts :autofailover))
      autofailover-timeout  (or (opts :autofailover-timeout)  6)
      autofailover-maxcount (or (opts :autofailover-maxcount) 3)
      disrupt-count         (or (opts :disrupt-count) 1)
      client                (clients/set-client addclient delclient dcpclient)

      nemesis               (cbnemesis/couchbase)
      checker               (checker/compose
                             (merge
                              {:set (checker/set)}
                              (if (opts :perf-graphs)
                                {:perf (checker/perf)})))
      generator             (gen/phases
                             (->> (range)
                                  (map (fn [x] {:type :invoke :f :add :value x}))
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
      addclient (cbclients/batch-insert-pool)
      delclient nil
      dcpclient (cbclients/dcp-client)
      ;; Around 100 Kops per node should be sufficient to trigger cursor dropping with
      ;; 100 MB per node bucket quota and ep_cursor_dropping_upper_mark reduced to 30%.
      ;; Since we need the first 2/3 of the ops to cause cursor dropping, we need 150 K
      ;; per node
      oplimit       (or (opts :oplimit)
                        (* (count (opts :nodes)) 150000))
      custom-cursor-drop-marks [20 30]
      concurrency   250
      batch-size    20
      pool-size     8
      replicas      (or (opts :replicas) 0)
      replicate-to  (or (opts :replicate-to) 0)
      autofailover  (if (nil? (opts :autofailover)) true (opts :autofailover))
      autofailover-timeout     (or (opts :autofailover-timeout)  6)
      autofailover-maxcount    (or (opts :autofailover-maxcount) 3)
      client        (clients/set-client addclient delclient dcpclient)
      nemesis       (cbnemesis/couchbase)
      checker       (checker/compose
                     (merge
                      {:set (checker/set)}
                      (if (opts :perf-graphs)
                        {:perf (checker/perf)})))

      generator     (gen/phases
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
      addclient     (cbclients/batch-insert-pool)
      delclient     (cbclients/basic-client)
      dcpclient     (cbclients/dcp-client)

      ;; Around 100 Kops per node should be sufficient to trigger cursor dropping with
      ;; 100 MB per node bucket quota and ep_cursor_dropping_upper_mark reduced to 30%.
      oplimit       (or (opts :oplimit)
                        (+ (* (count (opts :nodes)) 100000) 50000))
      custom-cursor-drop-marks [20 30]
      concurrency   250
      batch-size    50
      pool-size     4
      replicas      (or (opts :replicas) 0)
      replicate-to  (or (opts :replicate-to) 0)
      autofailover  (if (nil? (opts :autofailover)) true (opts :autofailover))
      autofailover-timeout     (or (opts :autofailover-timeout)  6)
      autofailover-maxcount    (or (opts :autofailover-maxcount) 3)
      client        (clients/set-client addclient delclient dcpclient)
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
