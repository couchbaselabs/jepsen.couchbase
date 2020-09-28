(ns couchbase.workload.legacy
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn error fatal]]
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
  (op [_ testData process]
      (if (= @(:control-atom testData) :continue)
        (gen/op gen testData process))))

(gen/defgenerator set-atom-stop
  []
  []
  (gen/op [_ testData process]
          (compare-and-set! (:control-atom testData) :continue :stop)
          nil))

(gen/defgenerator start-timeout
  [timeout]
  [timeout]
  (op [_ testData process]
      (future (do (Thread/sleep timeout)
                  (if (compare-and-set! (:control-atom testData) :continue :abort)
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
                                     :durability-level (util/random-durability-level (:durability opts))
                                     :json (:use-json-docs opts)}))
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
   stop-start-targeter   (cbnemesis/start-stop-targeter)
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
                                       (gen/sleep 20)
                                       ; We might need to rebalance the cluster if we're testing
                                       ; against ephemeral so we can read data back see MB-36800
                                       {:type :info
                                        :f :rebalance-cluster}]
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
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :start}

                                       (gen/sleep disrupt-time)

                                       {:type :info
                                        :f :start-process
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :stop}

                                       (gen/sleep 5)
                                       {:type :info
                                        :f :recover
                                        :recovery-type recovery-type}
                                       (gen/sleep 10)]
                                      client-gen)
                 :hard-reboot
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type :info
                                        :f :hard-reboot
                                        :targeter cbnemesis/basic-nodes-targeter
                                        :target-count disrupt-count}
                                       (gen/sleep 60)
                                       ; We might need to rebalance the cluster if we're testing
                                       ; against ephemeral so we can read data back see MB-36800
                                       {:type :info
                                        :f :rebalance-cluster}]
                                      client-gen)
                 :suspend-process
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type           :info
                                        :f              :halt-process
                                        :targeter       stop-start-targeter
                                        :target-process (:process-to-suspend opts)
                                        :target-count   disrupt-count
                                        :target-action  :start}
                                       (gen/sleep (:process-suspend-time opts))
                                       {:type :info
                                        :f :continue-process
                                        :target-process  (:process-to-suspend opts)
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :stop}
                                       (gen/sleep 5)]
                                      client-gen))
               (gen/clients (gen/once {:type :invoke :f :read :value nil})))))

(defn set-add-gen
  "Standard set-add operations with value denoting the set and insert-value
  denoting the value to be added to that set"
  [opts]
  (map
   (fn [x]
     {:type :invoke,
      :f :add,
      :value x,
      :insert-value x,
      :replicate-to (:replicate-to opts),
      :persist-to (:persist-to opts),
      :durability-level
      (util/random-durability-level (:durability opts)),
      :json (:use-json-docs opts)})
   (range (opts :doc-count 10000))))

(defn set-upsert-gen
  "Standard set-upsert operations with value denoting the set and insert-value
  denoting the value to be upserted to that set"
  [opts]
  (map
   (fn [x]
     {:type :invoke,
      :f :upsert,
      :value x,
      :insert-value (inc x),
      :replicate-to (:replicate-to opts),
      :persist-to (:persist-to opts),
      :durability-level
      (util/random-durability-level (:durability opts)),
      :json (:use-json-docs opts)})
   (range (opts :doc-count 10000))))

(defn set-delete-gen
  "Standard set-delete operations with value denoting the set. We try to delete the
  first half of the documents (0 to doc-count/2, and doc-count defaults to 10000 in all
  set operations if not given)"
  [opts]
  (map
   (fn [x]
     {:type :invoke,
      :f :del,
      :value x,
      :replicate-to (:replicate-to opts),
      :persist-to (:persist-to opts),
      :durability-level
      (util/random-durability-level (:durability opts))})
   (range 0 (quot (opts :doc-count 10000) 2))))

(defn upset-workload
  "Generic set upsert workload. We model the set with a bucket, adding(or upserting) an item to the
  set corresponds to inserting(or upserting) a key . To read the set we use a dcp client to
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
                    :set (cbchecker/set-upsert-checker)
                    :sanity (cbchecker/sanity-check)}
                   (if (opts :perf-graphs)
                     {:perf (checker/perf)})))
   client-gen    (gen/seq (concat (set-add-gen opts) (set-upsert-gen opts)))
   generator     (gen/phases
                  (do-n-nemesis-cycles (:cycles opts) [] client-gen)
                  (gen/clients (gen/once {:type :invoke :f :read :value nil :check :upsert-set-checker})))))

(defn upset-kill-workload
  "Set upsert workload that repeatedly kills memcached while hammering inserts and upserts against
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
   stop-start-targeter   (cbnemesis/start-stop-targeter)
   control-atom          (atom :continue)
   checker               (checker/compose
                          (merge
                           {:timeline (timeline/html)
                            :set (cbchecker/set-upsert-checker)
                            :sanity (cbchecker/sanity-check)}
                           (if (opts :perf-graphs)
                             {:perf (checker/perf)})))
   client-gen (gen/seq (concat (set-add-gen opts) (set-upsert-gen opts)))
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
                                       (gen/sleep 20)
                                         ; We might need to rebalance the cluster if we're testing
                                         ; against ephemeral so we can read data back see MB-36800
                                       {:type :info
                                        :f :rebalance-cluster}]
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
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :start}

                                       (gen/sleep disrupt-time)

                                       {:type :info
                                        :f :start-process
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :stop}

                                       (gen/sleep 5)
                                       {:type :info
                                        :f :recover
                                        :recovery-type recovery-type}
                                       (gen/sleep 10)]
                                      client-gen)
                 :hard-reboot
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type :info
                                        :f :hard-reboot
                                        :targeter cbnemesis/basic-nodes-targeter
                                        :target-count disrupt-count}
                                       (gen/sleep 60)
                                         ; We might need to rebalance the cluster if we're testing
                                         ; against ephemeral so we can read data back see MB-36800
                                       {:type :info
                                        :f :rebalance-cluster}]
                                      client-gen)
                 :suspend-process
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type           :info
                                        :f              :halt-process
                                        :targeter       stop-start-targeter
                                        :target-process (:process-to-suspend opts)
                                        :target-count   disrupt-count
                                        :target-action  :start}
                                       (gen/sleep (:process-suspend-time opts))
                                       {:type :info
                                        :f :continue-process
                                        :target-process  (:process-to-suspend opts)
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :stop}
                                       (gen/sleep 5)]
                                      client-gen))
               (gen/clients (gen/once {:type :invoke :f :read :value nil :check :upsert-set-checker})))))

(defn set-delete-workload
  "Generic set delete workload. We model the set with a bucket, adding(or deleting) an item to the
  set corresponds to inserting(or deleting) a key . To read the set we use a dcp client to
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
                    :set (cbchecker/extended-set-checker)
                    :sanity (cbchecker/sanity-check)}
                   (if (opts :perf-graphs)
                     {:perf (checker/perf)})))
   client-gen    (gen/seq (concat (set-add-gen opts) (set-delete-gen opts)))
   generator     (gen/phases
                  (do-n-nemesis-cycles (:cycles opts) [] client-gen)
                  (gen/clients (gen/once {:type :invoke :f :read :value nil})))))

(defn set-delete-kill-workload
  "Set delete workload that repeatedly kills memcached while hammering inserts and deletes against
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
   stop-start-targeter   (cbnemesis/start-stop-targeter)
   control-atom          (atom :continue)
   checker               (checker/compose
                          (merge
                           {:timeline (timeline/html)
                            :set (cbchecker/extended-set-checker)
                            :sanity (cbchecker/sanity-check)}
                           (if (opts :perf-graphs)
                             {:perf (checker/perf)})))
   client-gen (gen/seq (concat (set-add-gen opts) (set-delete-gen opts)))
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
                                       (gen/sleep 20)
                                         ; We might need to rebalance the cluster if we're testing
                                         ; against ephemeral so we can read data back see MB-36800
                                       {:type :info
                                        :f :rebalance-cluster}]
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
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :start}

                                       (gen/sleep disrupt-time)

                                       {:type :info
                                        :f :start-process
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :stop}

                                       (gen/sleep 5)
                                       {:type :info
                                        :f :recover
                                        :recovery-type recovery-type}
                                       (gen/sleep 10)]
                                      client-gen)
                 :hard-reboot
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type :info
                                        :f :hard-reboot
                                        :targeter cbnemesis/basic-nodes-targeter
                                        :target-count disrupt-count}
                                       (gen/sleep 60)
                                         ; We might need to rebalance the cluster if we're testing
                                         ; against ephemeral so we can read data back see MB-36800
                                       {:type :info
                                        :f :rebalance-cluster}]
                                      client-gen)
                 :suspend-process
                 (do-n-nemesis-cycles cycles
                                      [(gen/sleep 10)
                                       {:type           :info
                                        :f              :halt-process
                                        :targeter       stop-start-targeter
                                        :target-process (:process-to-suspend opts)
                                        :target-count   disrupt-count
                                        :target-action  :start}
                                       (gen/sleep (:process-suspend-time opts))
                                       {:type :info
                                        :f :continue-process
                                        :target-process  (:process-to-suspend opts)
                                        :targeter stop-start-targeter
                                        :target-count disrupt-count
                                        :target-action :stop}
                                       (gen/sleep 5)]
                                      client-gen))
               (gen/clients (gen/once {:type :invoke :f :read :value nil})))))

;; ==================
;; Counter workloads
;; ==================

; Functions to return default op for the counter workload
(def counter-add {:type :invoke :f :add :value 1})
(def counter-sub {:type :invoke :f :add :value -1})
(def counter-read {:type :invoke :f :read})

(defn set-durability-level
  "Helper function to add durability settings to each op"
  [options gen]
  (gen/map (fn [op]
             (assoc op :replicate-to (:replicate-to options)
                    :persist-to (:persist-to options)
                    :durability-level (util/random-durability-level (:durability options))))
           gen))

(defn counter-add-workload
  "Workload that treats one document as a counter and performs
  increments to it while also performing reads"
  [opts]
  (let-and-merge
   opts
   cycles        (opts :cycles 1)
   client        (clients/counter-client)
   concurrency   250
   pool-size     4
   replicas      (opts :replicas 0)
   replicate-to  (opts :replicate-to 0)
   persist-to    (opts :persist-to 0)
   autofailover  (opts :autofailover true)
   autofailover-timeout  (opts :autofailover-timeout 6)
   autofailover-maxcount (opts :autofailover-maxcount 3)
   init-counter-value (opts :init-counter-value 0)
   control-atom  (atom :continue)
   checker   (checker/compose
              {:timeline (timeline/html)
               :counter  (checker/counter)})
   client-generator  (->> (repeat 10 counter-add)
                          (cons counter-read)
                          gen/mix
                          (gen/delay 1/50)
                          (set-durability-level opts))
   generator (do-n-nemesis-cycles cycles [(gen/sleep 5)] client-generator)))

(defn counter-workload
  "Workload that treats one key as a counter which starts at a value x which
  is modified using increments or decrements"
  [opts]
  (let-and-merge
   opts
   cycles        (opts :cycles 1)
   client        (clients/counter-client)
   concurrency   250
   pool-size     4
   replicas      (opts :replicas 0)
   replicate-to  (opts :replicate-to 0)
   persist-to    (opts :persist-to 0)
   autofailover  (opts :autofailover true)
   autofailover-timeout  (opts :autofailover-timeout 6)
   autofailover-maxcount (opts :autofailover-maxcount 3)
   init-counter-value (opts :init-counter-value 1000000)
   control-atom  (atom :continue)
   checker   (checker/compose
              {:timeline (timeline/html)
               :counter  (cbchecker/sanity-counter)})
   client-generator (->> (take 100 (cycle [counter-add counter-sub]))
                         (cons counter-read)
                         gen/mix
                         (set-durability-level opts))
   generator (do-n-nemesis-cycles cycles [(gen/sleep 10)] client-generator)))

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
