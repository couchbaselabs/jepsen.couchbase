(ns couchbase.workload.failover
  (:require [couchbase
             [nemesis :as nemesis]
             [workload :as workload]]
            [jepsen.generator :as gen]))

(defn nemesis-cycle
  [opts]
  [(gen/sleep 5)
   {:type :info
    :f :failover
    :failover-type (opts :failover-type)
    :targeter nemesis/basic-nodes-targeter
    :target-count (opts :disrupt-count)}
   (gen/sleep 10)
   {:type :info
    :f :recover
    :recovery-type (opts :recovery-type)}
   (gen/sleep 5)])

(defn nemesis-gen
  [opts]
  [(repeatedly (opts :cycles) #(nemesis-cycle opts))
   (gen/sleep 10)
   {:type :stop-test}])

(defn workload-opts [opts]
  {:workload-type :register
   :disrupt-time (:disrupt-time opts 30)
   :failover-type (:failover-type opts :hard)})

(defn workload-fn
  "Failover and recover nodes"
  [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis (nemesis/couchbase)
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
