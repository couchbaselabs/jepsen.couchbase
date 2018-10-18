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
                       cas      (if document (.cas document) nil)
                       value    (if document (.content document) nil)
                       kvpair   (independent/tuple rawkey value)]
                   (assoc op :type :ok :value kvpair :cas cas))
                 (catch java.lang.RuntimeException _
                   (assoc op :type :fail)))

        :write (let [value         (long opval)
                     document      (JsonLongDocument/create opkey value)
                     replicate-to  (replicate-to (:replicate-to test))
                     cas           (->> (.upsert conn document replicate-to)
                                        (.cas))]
                 (assoc op :type :ok :cas cas)))))

  (teardown! [this test])

  (close! [_ test]
    (if (= 0 (swap! (:active-clients test) dec))
      (do
        (info "I am last client, close global connection")
        (try+
          (util/close-connection @(:connection test))
          (catch java.lang.RuntimeException _
              (info "Error closing connection, ignoring")))
        (reset! (:connection test) nil)
        (try+
          (Thread/sleep 1000)
          (catch java.lang.InterruptedException _
            (info "Ignoring interrupt exception")))
        (info "Connection torn down")))))

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
          :nemesis ((opts :nemesis))
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
                                   (gen/limit   10000))))
                          (gen/nemesis (nemesis/get-generator (:nemesis opts)))
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
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--custom-vbucket-count NUM_VBUCKETS"
     "Set the number of vbuckets (default 1024)"
    :parse-fn #(Integer/parseInt %)
    :default false]])

(defn -main
  "Handle command line arguments"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  cb-test
                                         :opt-spec extra-cli-options})
                   (cli/serve-cmd))
            args))
