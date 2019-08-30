(ns couchbase.cbclients
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase.util :as util])
  (:import com.couchbase.client.java.Cluster
           com.couchbase.client.java.env.ClusterEnvironment
           com.couchbase.client.core.env.IoConfig
           com.couchbase.client.core.env.TimeoutConfig
           com.couchbase.client.dcp.Client
           com.couchbase.client.dcp.ControlEventHandler
           com.couchbase.client.dcp.DataEventHandler
           com.couchbase.client.dcp.StreamFrom
           com.couchbase.client.dcp.StreamTo
           com.couchbase.client.dcp.config.DcpControl$Names
           com.couchbase.client.dcp.message.DcpDeletionMessage
           com.couchbase.client.dcp.message.DcpExpirationMessage
           com.couchbase.client.dcp.message.DcpMutationMessage
           com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest
           com.couchbase.client.dcp.message.MessageUtil
           com.couchbase.client.dcp.message.RollbackMessage
           com.couchbase.client.dcp.state.StateFormat
           com.couchbase.transactions.Transactions
           com.couchbase.transactions.config.TransactionConfigBuilder
           com.couchbase.transactions.TransactionDurabilityLevel
           java.time.Duration))

;; Couchbase Java SDK setup

(defn new-client
  "Open a new connection to the cluster, returning a map with the cluster,
  bucket, and collection instances"
  [test]
  (info "Opening new client")
  (let [node       (->> test :nodes rand-nth util/get-connection-string)
        ioConfig   (.mutationTokensEnabled (IoConfig/builder) true)
        timeout    (-> (TimeoutConfig/builder)
                       (.kvTimeout  (:kv-timeout test))
                       (.connectTimeout (Duration/ofSeconds (:connect-timeout test))))
        env        (-> (ClusterEnvironment/builder node "Administrator" "abc123")
                       (.timeoutConfig timeout)
                       (.ioConfig ioConfig)
                       (.build))
        cluster    (Cluster/connect env)
        bucket     (.bucket cluster "default")
        collection (.defaultCollection bucket)
        txn-config (if (:transactions test)
                     (.build (TransactionConfigBuilder/create)))
        txn        (if (:transactions test)
                     (-> (Transactions/create cluster txn-config)))]
    {:cluster cluster :bucket bucket :collection collection :env env :txn txn}))

;; We want to operate with a large amount of jepsen clients in order to test
;; lots of keys simultaneously. However, couchbase server connections are
;; expensive, so we don't want to have to create a connection for each client.
;; We therefore create a pool of connections, such that each jepsen client is
;; bound to one couchbase client. We allocate the clients in a round robin
;; fashion, this means the jepsen clients for a given key will use different
;; connections, which is required to detect some linearizability issues where
;; different clients see different data.
(def client-pool (atom nil))
(defn get-client-from-pool
  [test]
  (locking client-pool
    (when-not @client-pool
      (reset! client-pool (->> (partial new-client test)
                               (repeatedly (test :pool-size))
                               (doall)
                               (cycle)))
      (Thread/sleep 5000)))
  (ffirst (swap-vals! client-pool rest)))

(defn shutdown-pool
  [test]
  (locking client-pool
    (when client-pool
      (doseq [client (take (test :pool-size) @client-pool)]
        (try
          (some-> client :txn .close)
          (catch Exception e
            (warn "Ignored exception while closing transactions:" e))))
      (doseq [client (take (test :pool-size) @client-pool)]
        (try
          (some-> client :cluster .shutdown)
          (catch Exception e
            (warn "Ignored exception while disconnecting from cluster:" e))))
      (doseq [client (take (test :pool-size) @client-pool)]
        (try
          (some-> client :env .shutdown)
          (catch Exception e
            (warn "Ignored exception while shutting down environment:" e))))
      (reset! client-pool nil))))

;; DCP client logic

(defn dcpRollbackHandler [{:keys [client store]} event]
  (let [descr (RollbackMessage/toString event)
        vbid  (RollbackMessage/vbucket event)
        seqno (RollbackMessage/seqno   event)
        oksub     (reify rx.functions.Action0
                    (call [_] (info descr "completed ok")))
        errorsub  (reify rx.functions.Action1
                    (call [_ e]
                      (reset! store :INVALID)
                      (throw e)))]
    (assert (not= @store :INVALID) "Store invalid")
    (info "DCPControlEventHandler got:" descr)
    (swap! store (partial remove #(and (= (:vbucket %) vbid) (> (:seqno %) seqno))))
    (.subscribe (.rollbackAndRestartStream @client vbid seqno) oksub errorsub)))

(defn dcpControlEventHandler [{:keys [client store idle] :as client-record}]
  (reify ControlEventHandler
    (onEvent [_ flowController event]
      (swap! idle min 0)
      (cond
        (DcpSnapshotMarkerRequest/is event) nil
        (RollbackMessage/is event) (dcpRollbackHandler client-record event)
        :else (do (reset! store :INVALID)
                  (throw (RuntimeException. (str "Unknown DCP control event: " event)))))
      (.ack flowController event)
      (.release event))))

(defn dcpMutationHandler [{:keys [store]} event]
  (assert (not= @store :INVALID) "Store invalid")
  (let [key (DcpMutationMessage/keyString event)
        vbid (DcpMutationMessage/partition event)
        seqno (DcpMutationMessage/bySeqno   event)]
    (swap! store conj {:key key :seqno seqno :key-status :exists :vbucket vbid})))

(defn dcpDeletionHandler [{:keys [store]} event]
  (assert (not= @store :INVALID) "Store invalid")
  (let [key (DcpDeletionMessage/keyString event)
        vbid (DcpDeletionMessage/partition event)
        seqno (DcpDeletionMessage/bySeqno   event)]
    (swap! store conj {:key key :seqno seqno :key-status :deleted :vbucket vbid})))

(defn dcpDataEventHandler [{:keys [store slow idle] :as client-record}]
  (reify DataEventHandler
    (onEvent [_ flowController event]
      (if @slow (Thread/sleep 30))
      (swap! idle min 0)
      (cond
        (DcpMutationMessage/is event) (dcpMutationHandler client-record event)
        (DcpDeletionMessage/is event) (dcpDeletionHandler client-record event)
        (DcpExpirationMessage/is event) (dcpDeletionHandler client-record event)
        :else (do (reset! store :INVALID)
                  (throw (RuntimeException. (str "Unknown DCP data event: " event)))))
      (.ack flowController event)
      (.release event))))

(defn start-streaming [{:keys [client] :as client-record} test]
  (let [server-version (util/get-version (first (test :nodes)))
        client-builder (-> (Client/configure)
                           (.hostnames (test :nodes))
                           (.controlParam DcpControl$Names/SUPPORTS_CURSOR_DROPPING true)
                           (.controlParam DcpControl$Names/CONNECTION_BUFFER_SIZE 10000)
                           (.bufferAckWatermark 75)
                           (.bucket "default"))]
    (if (or (>= (first server-version) 5)
            (zero? (first server-version)))
      (-> client-builder
          (.username "Administrator")
          (.password "abc123")))
    (reset! client (.build client-builder))
    (.controlEventHandler @client (dcpControlEventHandler client-record))
    (.dataEventHandler @client (dcpDataEventHandler client-record))
    (-> @client (.connect) (.await))
    (-> @client (.initializeState StreamFrom/BEGINNING StreamTo/INFINITY) (.await))
    (-> @client (.startStreaming (make-array Short 0)) (.await))))

(defn get-all-keys [{:keys [client store slow idle] :as client-record} test]
  (if-not @client
    (start-streaming client-record test))
  (reset! slow false)
  (while (< @idle 10)
    (Thread/sleep 100)
    (swap! idle inc))
  (info "Finished getting mutations, disconnecting...")
  (-> @client (.disconnect) (.await))
  (info "Parsing results...")
  (if-not (= @store :INVALID)
    (->> (group-by :key @store)
         (vals)
         (map #(apply max-key :seqno %))
         (keep #(if (= (:key-status %) :exists) (:key %))))
    (throw (RuntimeException. "Store state invalid"))))

(defrecord NewSharedDcpClient [client store slow idle])

(defn dcp-client []
  (NewSharedDcpClient. (atom nil) (atom nil) (atom false) (atom 0)))
