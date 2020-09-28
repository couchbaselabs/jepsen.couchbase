(ns couchbase.workload.register
  (:require [couchbase
             [clients :as clients]
             [workload :as workload]]
            [jepsen
             [checker :as checker]
             [independent :as independent]
             [nemesis :as nemesis]]
            [jepsen.generator.pure :as gen]
            [knossos.model :as model]))

(defn nemesis-gen
  [opts]
  [(gen/sleep 5)
   (gen/repeat (opts :cycles) [(gen/sleep 20)])
   {:type :stop-test}])

(defn workload-opts [opts]
  {:workload-type :register})

(defn workload-fn [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis nemesis/noop
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
