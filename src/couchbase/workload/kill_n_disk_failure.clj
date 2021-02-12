(ns couchbase.workload.kill-n-disk-failure
  (:require [couchbase
             [nemesis :as nemesis]
             [workload :as workload]]
            [jepsen.generator :as gen]))

(defn nemesis-gen
  "Nemesis to replicate a scenario seen in MB-41257.
   We fail a nodes disk then kill its memcached process to ensure we don't lose
   data by warming up the failed node and rolling back replicas"
  [opts]
  ;; We only support a single-cycle, this is enforced by validate-opts
  ;; but assert here again in case that check is ever broken
  (assert (= 1 (:cycles opts)))
  [(gen/sleep 5)
   {:type :info
    :f :fail-disk
    :targeter nemesis/target-first-node
    :target-count (:disrupt-count opts)}
   (gen/sleep 5)
   {:type :info
    :f :kill-process
    :kill-process :memcached
    :targeter nemesis/target-first-node
    :target-count (opts :disrupt-count)}
   (gen/sleep 35)
   {:type :stop-test}])

(defn workload-opts [opts]
  {:workload-type :register
   :disrupt-time (:disrupt-time opts 20)})

(defn workload-fn
  "Simulate a disk failure then memcached crashing. This workload will
   not function correctly with docker containers or cluster-run nodes."
  [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis (nemesis/couchbase)
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
