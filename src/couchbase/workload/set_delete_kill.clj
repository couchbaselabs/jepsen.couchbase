(ns couchbase.workload.set-delete-kill
  (:require [couchbase
             [clients :as clients]
             [nemesis :as nemesis]
             [util :as util]
             [workload :as workload]]
            [jepsen.generator :as gen]))

;; Action Helpers

(defn kill-memcached-slow-disk-cycle
  [opts]
  [(gen/sleep 10)
   {:type :info
    :f :slow-disk
    :targeter nemesis/target-all-test-nodes}
   (gen/sleep 4)
   {:type :info
    :f :kill-process
    :kill-process :memcached
    :targeter nemesis/basic-nodes-targeter
    :target-count (opts :disrupt-count)}
   {:type :info
    :f :reset-disk
    :targeter nemesis/target-all-test-nodes}
   (gen/sleep 10)])

(defn kill-memcached-cycle
  [opts]
  [(gen/sleep 10)
   {:type :info
    :f :kill-process
    :kill-process :memcached
    :targeter nemesis/basic-nodes-targeter
    :target-count (opts :disrupt-count)}
   (gen/sleep 20)
   ;; We might need to rebalance the cluster if we're testing
   ;; against ephemeral so we can read data back see MB-36800
   {:type :info
    :f :rebalance-cluster}])

(defn kill-ns-server-cycle
  [opts]
  [(gen/sleep 10)
   {:type :info
    :f :kill-process
    :kill-process :ns-server
    :targeter nemesis/basic-nodes-targeter
    :target-count (opts :disrupt-count)}
   (gen/sleep 20)])

(defn kill-babysitter-cycle
  [opts]
  (let [targeter (nemesis/start-stop-targeter)]
    [(gen/sleep 5)
     {:type :info
      :f :kill-process
      :kill-process :babysitter
      :targeter targeter
      :target-count (opts :disrupt-count)
      :target-action :start}
     (gen/sleep (opts :disrupt-time))
     {:type :info
      :f :start-process
      :targeter targeter
      :target-count (opts :disrupt-count)
      :target-action :stop}
     (gen/sleep 5)
     {:type :info
      :f :recover
      :recovery-type (opts :recovery-type)}
     (gen/sleep 10)]))

(defn hard-reboot-cycle
  [opts]
  [(gen/sleep 10)
   {:type :info
    :f :hard-reboot
    :targeter nemesis/basic-nodes-targeter
    :target-count (opts :disrupt-count)}
   (gen/sleep 60)
   ;; We might need to rebalance the cluster if we're testing
   ;; against ephemeral so we can read data back see MB-36800
   {:type :info
    :f :rebalance-cluster}])

(defn suspend-process-cycle
  [opts]
  (let [targeter (nemesis/start-stop-targeter)]
    [(gen/sleep 10)
     {:type :info
      :f :halt-process
      :target-process (opts :process-to-suspend)
      :targeter targeter
      :target-count (opts :disrupt-count)
      :target-action :start}
     (gen/sleep (:process-suspend-time opts))
     {:type :info
      :f :continue-process
      :target-process (:process-to-suspend opts)
      :targeter-process (:process-to-suspend opts)
      :targeter targeter
      :target-count (opts :disrupt-count)
      :target-action :stop}
     (gen/sleep 5)]))

(defn nemesis-cycle
  [opts]
  (case (:scenario opts)
    :kill-memcached-on-slow-disk (kill-memcached-slow-disk-cycle opts)
    :kill-memcached (kill-memcached-cycle opts)
    :kill-ns-server (kill-ns-server-cycle opts)
    :kill-babysitter (kill-babysitter-cycle opts)
    :hard-reboot (hard-reboot-cycle opts)
    :suspend-process (suspend-process-cycle opts)
    (throw (RuntimeException. "set-delete-kill workload requires a scenario"))))

(defn nemesis-gen
  [opts]
  [(repeatedly (opts :cycles) #(nemesis-cycle opts))
   (gen/sleep 10)
   {:type :stop-test}])

;; Client Gen

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

;; Combined Gen

(defn combined-gen
  [opts]
  (workload/wrap-set-generator
   (gen/phases
    (workload/wrap-generators (client-gen opts) (nemesis-gen opts))
    {:f :read :value nil})))

;; Workload Setup

(defn workload-opts [opts]
  {:workload-type :set
   :custom-vbucket-count (:custom-vbucket-count opts 64)
   :disrupt-time (:disrupt-time opts 30)})

(defn workload-fn [opts]
  (merge
   (workload/set-common opts :extended)
   {:pure-generators true
    :concurrency 1000
    :nemesis (nemesis/couchbase)
    :generator (combined-gen opts)}))
