(ns couchbase.nemesis
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [couchbase [util      :as util]]
            [jepsen    [generator :as gen]
                       [nemesis   :as nemesis]]))

(defn disconnect-two
  "Introduce a partition that prevents two nodes from communicating"
  [first second]
  (let [targeter (fn [nodes] {first #{second}, second #{first}})]
    (nemesis/partitioner targeter)))

(defn failover
  "Actively failover the node returned by targeter through a rest-api call to
  node healthy"
  ([] (failover rand-nth))
  ([targeter]
   (nemesis/node-start-stopper
     targeter
     (fn start [test node]
       (let [endpoint "/controller/failOver"
             params   (str "otpNode=ns_1@" node)]
         (util/rest-call endpoint params)
         [:failed-over node]))

     (fn stop [test node]
       (let [nodes    (test :nodes)
             endpoint "/controller/setRecoveryType"
             params   (->> node
                           (str "ns_1@")
                           (format "otpNode=%s&recoveryType=delta"))]
         (util/rest-call endpoint params)
         (util/rebalance nodes))))))

(defn partition-then-failover
  "Introduce a partition such that two nodes cannot communicate, then failover
  one of those nodes. This Nemesis is really just a constructor, during setup
  we create and return a new jepsen.nemesis/compose, but since we need the test
  parameters to do so we can't do this in advance."
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

    ; These are never called, since we replace this nemesis with jepsen.nemesis/compose
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
      (let [nodes            (test :nodes)
            endpoint         "/controller/rebalance"
            ejected-nodes    (->> target
                                  (str "ns_1@"))
            known-nodes      (->> nodes
                                  (map #(str "ns_1@" %))
                                  (str/join ","))
            rebalance-params (str "ejectedNodes=" ejected-nodes
                                  "&knownNodes="  known-nodes)

            healthy          (first (remove #(= target %) nodes))
            status-endpoint  "/pools/default/rebalanceProgress"
            rebalance-status #(util/rest-call healthy status-endpoint nil)]
        (info (util/rest-call endpoint rebalance-params))

        (loop [status (rebalance-status)]
          (if (not= status "{\"status\":\"none\"}")
            (do
              (info "Rebalance status" status)
              (Thread/sleep 1000)
              (recur (rebalance-status)))
            (info "Rebalance complete")))))

    (fn stop [test node]
      (let [endpoint "/node/controller/doJoinCluster"
            nodes    (test :nodes)
            healthy  (->> nodes
                          (remove #(= node %))
                          (first))
            params   (str "clusterMemberHostIp=" healthy
                          "&clusterMemberPort=8091"
                          "&user=Administrator"
                          "&password=abc123")]
        (info (util/rest-call endpoint params))
        (util/rebalance nodes)))))

(defn noop [] nemesis/noop)

(def nemesies
  {"none"                    noop
   "failover-single"         failover
   "partition-single"        nemesis/partition-random-node
   "partition-then-failover" partition-then-failover
   "graceful-failover"       graceful-failover
   "rebalance-in-out"        rebalance-in-out})

(defn get-generator
  "Get a suitable generator for the given nemesis"
  [nemesis]
  (info "Nemesis is" nemesis)
  (info "Compare to" partition-then-failover)
  (condp = nemesis
    partition-then-failover
    (gen/seq [(gen/sleep 15)
              {:type :info :f :start-partition}
              (gen/sleep 5)
              {:type :info :f :start-failover}])

    noop
    (gen/seq [])

    (gen/seq (cycle [(gen/sleep 15)
                     {:type :info :f :start}
                     (gen/sleep 20)
                     {:type :info :f :stop}
                     (gen/sleep 4000)]))))

    
