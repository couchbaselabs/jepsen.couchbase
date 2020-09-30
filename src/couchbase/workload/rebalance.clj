(ns couchbase.workload.rebalance
  (:require [couchbase
             [nemesis :as nemesis]
             [workload :as workload]]
            [jepsen.generator :as gen]))

;; Action Helpers

(defn rebalance-out
  "Rebalance out node(s)"
  [targeter count]
  [(gen/sleep 5)
   {:type :info
    :f :rebalance-out
    :targeter targeter
    :target-count count
    :target-action :start}])

(defn rebalance-in
  "Rebalance previously removed node(s) back in"
  [targeter count]
  [(gen/sleep 5)
   {:type :info
    :f :rebalance-in
    :targeter targeter
    :target-count count
    :target-action :stop}])

(defn swap-rebalance
  "Perform a swap-rebalance"
  [targeter count]
  [(gen/sleep 5)
   {:type :info
    :f :swap-rebalance
    :targeter targeter
    :target-count count
    :target-action :swap}])

(defn fail-rebalance
  "Fail a rebalance by killing memcached"
  [targeter count]
  [(gen/sleep 5)
   {:type :info
    :f :fail-rebalance
    :targeter targeter
    :target-count count}])

;; Scenario Nemesis Generators

(defn sequential-rebalance-cycle
  [opts targeter]
  [(repeatedly (opts :disrupt-count) #(rebalance-out targeter 1))
   (gen/sleep 5)
   (repeatedly (opts :disrupt-count) #(rebalance-in targeter 1))
   (gen/sleep 5)])

(defn sequential-rebalance-gen
  [opts]
  (let [targeter (nemesis/start-stop-targeter)]
    [(repeatedly (opts :cycles) #(sequential-rebalance-cycle opts targeter))
     (gen/sleep 10)
     {:type :stop-test}]))

(defn bulk-rebalance-cycle
  [opts targeter]
  [(rebalance-out targeter (opts :disrupt-count))
   (rebalance-in targeter (opts :disrupt-count))
   (gen/sleep 5)])

(defn bulk-rebalance-gen
  [opts]
  (let [targeter (nemesis/start-stop-targeter)]
    [(repeatedly (opts :cycles) #(bulk-rebalance-cycle opts targeter))
     (gen/sleep 10)
     {:type :stop-test}]))

(defn swap-rebalance-gen
  [opts]
  (let [targeter (nemesis/start-stop-targeter)]
    [(rebalance-out targeter (opts :disrupt-count))
     (repeatedly (opts :cycles) #(swap-rebalance targeter (opts :disrupt-count)))
     (gen/sleep 10)
     {:type :stop-test}]))

(defn fail-rebalance-gen
  [opts]
  ;; Nodes don't get removed on a failed rebalance, so use basic targeter
  (let [targeter nemesis/basic-nodes-targeter]
    [(repeatedly (opts :cycles) #(fail-rebalance targeter (opts :disrupt-count)))
     (gen/sleep 15)
     {:type :stop-test}]))

;; Workload Setup

(defn nemesis-gen [opts]
  (case (:scenario opts)
    :sequential-rebalance-out-in (sequential-rebalance-gen opts)
    :bulk-rebalance-out-in (bulk-rebalance-gen opts)
    :swap-rebalance (swap-rebalance-gen opts)
    :fail-rebalance (fail-rebalance-gen opts)
    (throw (RuntimeException. "Rebalance workload requires a scenario"))))

(defn workload-opts [opts]
  {:workload-type :register})

(defn workload-fn
  "Rebalance nodes according to the selected scenario"
  [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis (nemesis/couchbase)
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
