(ns couchbase.workload.set-delete
  (:require [couchbase
             [clients :as clients]
             [util :as util]
             [workload :as workload]]
            [jepsen
             [nemesis :as nemesis]]
            [jepsen.generator.pure :as gen]))

(defn client-gen-cycle
  [opts offset]
  (concat (map #(gen/once {:f :add
                           :value %
                           :replicate-to (:replicate-to opts)
                           :persist-to (:persist-to opts)
                           :durability-level (util/random-durability-level
                                              (:durability opts))
                           :json (:use-json-docs opts)})
               (range (* 10000 offset)
                      (+ (* 10000 offset) 10000)))
          (map #(gen/once {:f :del
                           :value %
                           :replicate-to (:replicate-to opts)
                           :persist-to (:persist-to opts)
                           :durability-level (util/random-durability-level
                                              (:durability opts))})
               ;; Delete first half of the range
               (range (* 10000 offset)
                      (+ (* 10000 offset) 5000)))))

(defn client-gen
  [opts]
  (map #(client-gen-cycle opts %) (range)))

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
   (workload/set-common opts :extended)
   {:pure-generators true
    :concurrency 250
    :nemesis nemesis/noop
    :generator (combined-gen opts)}))
