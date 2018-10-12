(ns couchbase.nemesis
  (:require [clojure.tools.logging :refer :all]
            [couchbase [util    :as util]]
            [jepsen    [nemesis :as nemesis]]))

(defn disconnect-two
  "Introduce a partition that prevents two nodes from communicating"
  [first second]
  (let [targeter (fn [nodes] {first #{second}, second #{first}})]
    (nemesis/partitioner targeter)))

(defn failover
  "Actively failover the node returned by targeter through a rest-api call to
  node healthy"
  [targeter healthy]
  (nemesis/node-start-stopper (fn [& args] healthy)
                              (fn start [t n]
                                (let [target   (targeter)
                                      endpoint "/controller/failOver"
                                      params   (str "otpNode=ns_1@" target)]
                                  (util/rest-call endpoint params)
                                  [:failed-over target]))
                              (fn stop [t n])))

(defn partition-then-failover
  "Introduce a partition such that two nodes cannot communicate, then failover
  one of those nodes"
  [opts]
  (let [[first, second, healthy] (->> (:nodes opts)
                                      (shuffle)
                                      (take 3))
        targeter                 (fn [& args] first)]
    (info "Nemesis is going to partition" first "and" second
          "then failover" first)
    (nemesis/compose {{:start-stage1 :start} (disconnect-two first second)
                      {:start-stage2 :start} (failover targeter healthy)})))

(defn failover-single
  "Failover a random node"
  [opts]
  (let [nodes     (:nodes opts)
        healthy   (rand-nth nodes)
        others    (remove #(= healthy %) nodes)
        targeter  #(rand-nth others)]
    (failover targeter healthy)))

(def nemesies
  {"none"                    (fn [& args] nemesis/noop)
   "failover-single"         failover-single
   "partition-single"        (fn [& args] (nemesis/partition-random-node))
   "partition-then-failover" partition-then-failover})
