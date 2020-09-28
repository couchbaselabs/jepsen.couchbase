(ns couchbase.workload.set
  (:require [couchbase
             [clients :as clients]
             [util :as util]
             [workload :as workload]]
            [jepsen
             [nemesis :as nemesis]]
            [jepsen.generator.pure :as gen]))

(defn client-gen
  [opts]
  (map (fn [x] {:f :add
                :value x
                :replicate-to (:replicate-to opts)
                :persist-to (:persist-to opts)
                :durability-level (util/random-durability-level
                                   (:durability opts))
                :json (:use-json-docs opts)})
       (range)))

(defn nemesis-gen
  [opts]
  [(gen/repeat (opts :cycles) [(gen/sleep 20)])
   (gen/sleep 10)
   {:type :stop-test}])

(defn combined-gen
  [opts]
  (workload/wrap-set-generator
   (gen/phases
    (workload/wrap-generators (client-gen opts) (nemesis-gen opts))
    {:f :read :value nil})))

(defn workload-opts [opts]
  {:workload-type :set})

(defn workload-fn [opts]
  (merge
   (workload/set-common opts :basic)
   {:pure-generators true
    :concurrency 250
    :nemesis nemesis/noop
    :generator (combined-gen opts)}))
