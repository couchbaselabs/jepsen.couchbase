(ns couchbase.workload.counter-add
  (:require [couchbase
             [clients :as clients]
             [util :as util]
             [workload :as workload]]
            [jepsen
             [checker :as checker]
             [nemesis :as nemesis]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator.pure :as gen]))

(defn counter-add [test ctx]
  {:f :add
   :value 1
   :replicate-to (:replicate-to test)
   :persist-to (:persist-to test)
   :durability-level (util/random-durability-level
                      (:durability test))})

(defn counter-read []
  {:f :read})

(defn client-gen [opts]
  (cond->> (gen/mix (conj (repeat 10 counter-add) counter-read))
    (pos? (:rate opts 0)) (gen/stagger (/ (:rate opts)))))

(defn nemesis-gen
  [opts]
  [(gen/repeat (opts :cycles) [(gen/sleep 5)])
   (gen/sleep 10)
   {:type :stop-test}])

(defn combined-gen
  [opts]
  (workload/wrap-generators (client-gen opts) (nemesis-gen opts)))

(defn workload-opts [opts]
  {:workload-type :counter
   :init-counter-value (:init-counter-value opts 0)})

(defn workload-fn [opts]
  {:pure-generators true
   :concurrency 250
   :client (clients/counter-client)
   :generator (combined-gen opts)
   :checker (checker/compose
             (merge
              {:timeline (timeline/html)
               :counter (checker/counter)}
              (if (opts :perf-graphs)
                {:perf (checker/perf)})))})
