(ns couchbase.workload.partition
  (:require [couchbase
             [nemesis :as nemesis]
             [workload :as workload]]
            [jepsen.generator.pure :as gen]))

(defn nemesis-cycle
  [opts]
  [(gen/sleep 5)
   {:type :info
    :f :isolate-completely
    :targeter nemesis/basic-nodes-targeter
    :target-count (opts :disrupt-count)}
   (gen/sleep (opts :disrupt-time))
   {:type :info
    :f :heal-network}
   (gen/sleep 2)
   {:type :info
    :f :recover
    :recovery-type (opts :recovery-type)}
   (gen/sleep 10)])

(defn nemesis-gen
  [opts]
  [(repeatedly (opts :cycles) #(nemesis-cycle opts))
   (gen/sleep 10)
   {:type :stop-test}])

(defn workload-opts [opts]
  {:workload-type :register
   :disrupt-time (:disrupt-time opts 20)})

(defn workload-fn
  "Partitions the network by isolating nodes from each other, will recover if
  a node is autofailovered."
  [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis (nemesis/couchbase)
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
