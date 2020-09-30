(ns couchbase.workload.partition-failover
  (:require [clojure.set :as set]
            [couchbase
             [nemesis :as nemesis]
             [workload :as workload]]
            [jepsen.generator :as gen]))

(defn nemesis-cycle
  [opts]
  (let [partition-nodes (->> opts :nodes shuffle (take 2))
        failover-target-node (rand-nth partition-nodes)
        failover-call-node (-> (set/difference (set (:nodes opts))
                                               (set partition-nodes))
                               (seq)
                               (rand-nth))]
    [(gen/sleep 5)
     {:type :info
      :f :isolate-two-nodes-from-each-other
      :targeter (constantly partition-nodes)}
     (gen/sleep 5)
     {:type :info
      :f :failover
      :targeter (constantly (list failover-target-node))
      :call-node (constantly failover-call-node)
      :failover-type (:failover-type opts)}
     (gen/sleep (:disrupt-time opts))
     {:type :info :f :heal-network}
     (gen/sleep 10)
     {:type :info
      :f :recover
      :recovery-type (:recovery-type opts)}]))

(defn nemesis-gen
  [opts]
  [(repeatedly (opts :cycles) #(nemesis-cycle opts))
   (gen/sleep 10)
   {:type :stop-test}])

(defn workload-opts [opts]
  {:workload-type :register
   :disrupt-time (:disrupt-time opts 20)
   :failover-type (:failover-type opts :hard)})

(defn workload-fn
  [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis (nemesis/couchbase)
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
