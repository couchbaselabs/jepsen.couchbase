(ns couchbase.nemesis
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [couchbase [util      :as util]]
            [jepsen    [control   :as c]
                       [generator :as gen]
                       [nemesis   :as nemesis]]))

(defn disconnect-two
  "Introduce a partition that prevents two nodes from communicating"
  [first second]
  (let [targeter (fn [nodes] {first #{second}, second #{first}})]
    (nemesis/partitioner targeter)))

(defn failover
  "Actively failover a node through the rest-api. Supports :delta, or
  :full recovery on nemesis stop."
  ([]         (failover rand-nth :delta))
  ([targeter] (failover targeter :delta))
  ([targeter recovery-type]
   (nemesis/node-start-stopper
     targeter
     (fn start [test node]
       (let [endpoint "/controller/failOver"
             params   (str "otpNode=ns_1@" node)]
         (util/rest-call endpoint params)
         [:failed-over node]))

     (fn stop [test node]
       (util/rest-call "/controller/setRecoveryType"
                       (format "otpNode=%s&recoveryType=%s"
                               (str "ns_1@" node)
                               (name recovery-type)))
       (util/rebalance (test :nodes))))))

(defn partition-then-failover
  "Introduce a partition such that two nodes cannot communicate, then failover
  one of those nodes. This Nemesis is only used as a constructor, during setup
  it replaces itself with a new nemesis constructed by jepsen.nemesis/compose"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (let [[first, second]    (->> (:nodes test)
                                    (shuffle)
                                    (take 2))
            disconnect-nemesis (disconnect-two first second)
            failover-nemesis   (failover (constantly first))
            combined-nemesis   (nemesis/compose
                                 {{:start-partition :start} disconnect-nemesis
                                  {:start-failover  :start} failover-nemesis})]
        (info "Nemesis is going to partition" first "and" second
              "then failover" first)
        (nemesis/setup-compat! combined-nemesis test nil)))

    ; These are never called, since we replace this nemesis during setup
    (invoke! [this test op])
    (teardown! [this test] this)))

(defn graceful-failover
  "Gracefully fail over a random node upon start. Perform :delta or
  :full node recovery on nemesis stop"
  ([]         (graceful-failover rand-nth :delta))
  ([targeter] (graceful-failover targeter :delta))
  ([targeter recovery-type]
   (nemesis/node-start-stopper targeter
     (fn start [test target]
       (let [endpoint   "/controller/startGracefulFailover"
             params     (str "otpNode=ns_1@" target)
             get-status #(util/rest-call "/pools/default/rebalanceProgress" nil)]
         (util/rest-call endpoint params)

         (loop [status (get-status)]
           (if (not= status "{\"status\":\"none\"}")
             (do
               (info "Graceful failover status" status)
               (Thread/sleep 1000)
               (recur (get-status)))
           (info "Graceful failover complete")))
         [:gracefully-failed-over target]))

     (fn stop [test node]
       (util/rest-call "/controller/setRecoveryType"
                       (format "otpNode=%s&recoveryType=%s"
                               (str "ns_1@" node)
                               (name recovery-type)))
       (util/rebalance (test :nodes))))))

(defn rebalance-in-out
  "Rebalance a node out of and back into the cluster"
  []
  (nemesis/node-start-stopper rand-nth
    (fn start [test target]
      (util/rebalance (test :nodes) target))

    (fn stop [test target]
      (let [nodes   (test :nodes)
            cluster (if (not= target (first nodes))
                      (first nodes)
                      (second nodes))]
        (c/on cluster (util/add-nodes [target]))
        (util/rebalance nodes)))))

(defn swap-rebalance
  "Upon each invocation swap rebalance two nodes. In order to have a free node
  to rebalance in we rebalance out one node upon nemesis setup"
  []
  (let [ejected (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this test]
        (reset! ejected (rand-nth (test :nodes)))
        (util/rebalance (test :nodes) @ejected)
        this)
      (invoke! [this test op]
        (if-not (= (:f op) :swap)
          (throw (RuntimeException. "Op for swap-rebalance nemesis must be :swap")))
        (let [nodes      (test :nodes)
              in-cluster (remove #(= @ejected %) nodes)
              to-remove  (rand-nth in-cluster)]
          (c/on (first in-cluster) (util/add-nodes [@ejected]))
          (util/rebalance nodes to-remove)
          (reset! ejected to-remove))
        (assoc op :type :info :status :done))
      (teardown! [this test] nil))))

(defn slow-dcp [DcpClient]
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :start (do
                 (reset! (:slow DcpClient) true)
                 (assoc op :type :info))
        :stop  (do
                 (reset! (:slow DcpClient) false)
                 (assoc op :type :info))))
    (teardown! [this test]
      (reset! (:slow DcpClient) false))))

(defn trigger-compaction []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (assert (= (:f op) :compact))
      (util/rest-call (rand-nth (test :nodes))
                      "/pools/default/buckets/default/controller/compactBucket"
                      "")
      op)
    (teardown! [this test])))
