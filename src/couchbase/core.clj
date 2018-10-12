(ns couchbase.core
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [couchbase [nemesis :as nemesis]
                       [util    :as util]]
            [jepsen [checker     :as checker]
                    [cli         :as cli]
                    [client      :as client]
                    [control     :as c]
                    [db          :as db]
                    [generator   :as gen]
                    [independent :as independent]
                    [tests       :as tests]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]])
  (:import com.couchbase.client.java.document.JsonLongDocument
           com.couchbase.client.java.ReplicateTo))

(defn couchbase
  "Initialise couchbase"
  []
  (reify
    db/DB
    (setup!    [_ test node] (util/setup-node))
    (teardown! [_ test node] (util/teardown))

    db/Primary
    (setup-primary! [_ test node]
      (util/setup-cluster test node)
      (reset! (test :connection) (util/get-connection test)))

    db/LogFiles
    (log-files [_ test node]
      (c/su (c/exec :tar :-C  "/opt/couchbase/var/lib/couchbase/logs"
                         :-cf "/opt/couchbase/var/lib/couchbase/logs.tar" "."))
      ["/opt/couchbase/var/lib/couchbase/logs.tar"])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 50)})

(def replicate-to [ReplicateTo/NONE ReplicateTo/ONE
                   ReplicateTo/TWO ReplicateTo/THREE])

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (swap! (:active-clients test) inc)
    (let [globalconn     @(:connection test)
          bucket         (:bucket globalconn)]
      (assoc this :conn bucket)))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[rawkey opval] (:value op)
          opkey          (format "testdoc%03d" rawkey)]
      (case (:f op)
        :read  (try+
                 (let [document (.get conn opkey JsonLongDocument)
                       value    (if document (.content document) nil)
                       kvpair   (independent/tuple rawkey value)]
                   (assoc op :type :ok :value kvpair))
                 (catch java.lang.RuntimeException _
                   (assoc op :type :fail)))

        :write (let [value         (long opval)
                     document      (JsonLongDocument/create opkey value)
                     replicate-to  (replicate-to (:replicate-to test))]
                 (.upsert conn document replicate-to)
                 (assoc op :type :ok)))))

  (teardown! [this test])

  (close! [_ test]
    (if (= 0 (swap! (:active-clients test) dec))
      (do
        (info "I am last client, close global connection")
        (util/close-connection @(:connection test))
        (reset! (:connection test) nil)))))

(defn cb-test
  "Run the test"
  [opts]
  (merge tests/noop-test
         opts
         {:name "Couchbase"
          :db (couchbase)
          :client (Client. nil)
          :connection (atom nil)
          :active-clients (atom 0)
          :model (model/register)
          :nemesis ((opts :nemesis) opts)
          :checker (checker/compose
                     {:perf  (checker/perf)
                      :indep (independent/checker
                               (checker/compose
                                 {:timeline (timeline/html)
                                  :linear   (checker/linearizable)}))})
          :generator (->> (independent/concurrent-generator
                            3
                            (range)
                            (fn [k]
                              (->> (gen/mix     [r w])
                                   (gen/stagger (/ (:rate opts)))
                                   (gen/limit   1000))))
                          (gen/nemesis
                            (if (= (opts :nemesis) nemesis/partition-then-failover)
                              (gen/seq  [(gen/sleep 15)
                                         {:type :info :f :start-stage1}
                                         (gen/sleep 5)
                                         {:type :info :f :start-stage2}])
                              (gen/seq  (cycle [(gen/sleep 15)
                                                {:type :info :f :start}
                                                (gen/sleep 30)
                                                {:type :info :f :stop}]))))
                          (gen/time-limit (:time-limit opts)))}))

(def extra-cli-options
  [[nil "--replicas NUMBER"
    "Number of replicas for the bucket"
    :parse-fn #({"0" 0 "1" 1 "2" 2 "3" 3} %)
    :default  1
    :validate [some? "Must be an integer between 0 and 3"]]
   [nil "--replicate-to NUMBER"
    "Number of replicas to ensure before confirming write"
    :parse-fn #({"0" 0 "1" 1 "2" 2 "3" 3} %)
    :default  0
    :validate [some? "Must be an integer between 0 and 3"]]
   [nil "--nemesis NEMESIS"
    "The nemesis to apply"
    :parse-fn nemesis/nemesies
    :default  (nemesis/nemesies "none")
    :validate [some? (cli/one-of nemesis/nemesies)]]
   [nil "--rate RATE"
    "Rate of requests to cluster"
    :parse-fn read-string
    :default  1
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]])

(defn -main
  "Handle command line arguments"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  cb-test
                                         :opt-spec extra-cli-options})
                   (cli/serve-cmd))
            args))
