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
  "Actively failover a node through the rest-api"
  ([] (failover rand-nth))
  ([targeter]
   (nemesis/node-start-stopper
     targeter
     (fn start [test node]
       (let [endpoint "/controller/failOver"
             params   (str "otpNode=ns_1@" node)]
         (util/rest-call endpoint params)
         [:failed-over node]))

     (fn stop [test node]))))

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
  "Gracefully fail over a random node upon start, perform delta node recovery
  upon nemesis stop"
  []
  (nemesis/node-start-stopper rand-nth
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
      (let [nodes    (test :nodes)
            endpoint "/controller/setRecoveryType"
            params   (->> node
                          (str "ns_1@")
                          (format "otpNode=%s&recoveryType=delta"))]
        (util/rest-call endpoint params)
        (util/rebalance nodes)))))

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
        (c/on cluster (util/add-nodes [node]))
        (util/rebalance nodes)))))



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
