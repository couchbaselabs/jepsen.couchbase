(ns couchbase.workload.kill
  (:require [couchbase
             [nemesis :as nemesis]
             [workload :as workload]]
            [jepsen.generator :as gen]))

;; Action Helpers

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
     (gen/sleep 5)]))

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
    :kill-memcached (kill-memcached-cycle opts)
    :kill-ns-server (kill-ns-server-cycle opts)
    :kill-babysitter (kill-babysitter-cycle opts)
    :hard-reboot (hard-reboot-cycle opts)
    :suspend-process (suspend-process-cycle opts)
    (throw (RuntimeException. "Kill workload requires a scenario"))))

(defn nemesis-gen
  [opts]
  [(repeatedly (opts :cycles) #(nemesis-cycle opts))
   (gen/sleep 10)
   {:type :stop-test}])

;; Workload Setup

(defn workload-opts [opts]
  {:workload-type :register
   :disrupt-time (:disrupt-time opts 20)})

(defn workload-fn
  "Rebalance nodes according to the selected scenario"
  [opts]
  (merge
   (workload/register-common opts)
   {:pure-generators true
    :nemesis (nemesis/couchbase)
    :generator (workload/wrap-generators
                (workload/register-client-gen opts)
                (nemesis-gen opts))}))
