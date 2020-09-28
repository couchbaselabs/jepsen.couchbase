(ns couchbase.workload.disk-failure
  (:require [couchbase
             [nemesis :as nemesis]
             [workload :as workload]]
            [jepsen.generator.pure :as gen]))

(defn nemesis-gen
  [opts]
  ;; We only support a single-cycle, this is enforced by validate-opts
  ;; but assert here again incase that check is ever broken
  (assert (= 1 (:cycles opts)))
  [(gen/sleep 10)
   {:type :info
    :f :fail-disk
    :targeter nemesis/basic-nodes-targeter
    :target-count (:disrupt-count opts)}
   (gen/sleep (+ 10 (:disrupt-time opts)))
   {:type :stop-test}])

(defn workload-opts [opts]
  {:workload-type :register
   :disrupt-time (:disrupt-time opts 20)})

(defn workload-fn
  "Simulate a disk failure. This workload will not function correctly with
  docker containers or cluster-run nodes."
  [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis (nemesis/couchbase)
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
