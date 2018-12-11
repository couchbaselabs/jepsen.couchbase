(ns couchbase.cbclients
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [couchbase.util :as util]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import com.couchbase.client.dcp.Client
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
           com.couchbase.client.java.document.JsonLongDocument
           com.couchbase.client.java.ReplicateTo
           rx.CompletableSubscriber
           rx.Observable
           rx.functions.Func1))


;; We want an easy way to switch out wrappers over the plain couchbase java sdk
;; clients so we create a basic interface. We use maybe-setup and maybe-close to
;; allow multiple jepsen clients share a single couchbase connection. We
;; implement the wrappers in such a way that the first call to maybe-setup will
;; create the connection. Other threads calling maybe-setup while setup is
;; running will block, until setup is complete. We track the difference of setup
;; and close calls, so that the last call to maybe-close can cleanly disconnect
;; from the couchbase cluster.
(defprotocol CouchbaseClient
  (maybe-setup [this test])
  (invoke      [this test optype opval])
  (maybe-close [this]))

(extend-protocol CouchbaseClient
  nil
  (maybe-setup [this test] nil)
  (invoke      [this test optype opval] nil)
  (maybe-close [this] nil))

;; Couchbase SDK wrappers

;; A basic wrapper that just passes ops through to the couchbase java sdk
(defrecord BasicClient [status cluster bucket]
  CouchbaseClient
  (maybe-setup [this test]
    (if (compare-and-set! status nil :preparing)  
      (do
        (reset! cluster (util/get-connection test))
        (reset! bucket  (.openBucket @cluster "default"))
        (reset! status 0))
      (while (= :preparing @status) (Thread/sleep 100)))
    (swap! status inc))

  (invoke [this test optype opval]
    (let [replicate-to (case (test :replicate-to)
                               0 ReplicateTo/NONE
                               1 ReplicateTo/ONE
                               2 ReplicateTo/TWO
                               3 ReplicateTo/THREE)]
      (case optype
        :get     (.get     @bucket opval JsonLongDocument)
        :insert  (.insert  @bucket opval replicate-to)
        :upsert  (.upsert  @bucket opval replicate-to)
        :replace (.replace @bucket opval replicate-to)
        :remove  (.remove  @bucket opval replicate-to))))

  (maybe-close [this]
    (when (= (swap! status dec) 0)
      (.close @bucket)
      (.disconnect @cluster))))


;; Perform a batch insertion operation of the provided operations on the
;; bucket. Return a map of key ids to new documents. Exceptions are silently
;; discarded, but only successfully inserted documents are present in the
;; returned map
(defn do-batch-insert [test bucket ops]
      (let [replicate-to (case (test :replicate-to)
                               0 ReplicateTo/NONE
                               1 ReplicateTo/ONE
                               2 ReplicateTo/TWO
                               3 ReplicateTo/THREE)]
           (->> (-> (java.util.ArrayList. ops)
                    (Observable/from)
                    (.flatMap
                      (reify Func1
                        (call [_ doc]
                          (-> (.insert bucket doc replicate-to)
                              (.onErrorResumeNext (Observable/empty))))))
                    (.toList)
                    (.toBlocking)
                    (.single))
                (map #(vector (.id %) %))
                (into {}))))


;; For the set workload We want to be able use the bulk operation support in
;; order to achieve higher throughput. We queue insert operations from multiple
;; jepsen threads, and once we achieve a given threshold dispatch them as a
;; single bulk operation.
(defrecord BatchInsertClient [status cluster bucket syncatom]
  CouchbaseClient
  (maybe-setup [this test]
    (if (compare-and-set! status nil :preparing)
      (do
        (reset! cluster (util/get-connection test))
        (reset! bucket  (->> (.openBucket @cluster "default") (.async)))
        (reset! syncatom [(promise)])
        (reset! status 0))
      (while (= :preparing @status) (Thread/sleep 100)))
    (swap! status inc))

  (invoke [this test optype opval]
    (assert (= optype :insert))
    (let [maybe-conj                (fn [vals op]
                                        (if (<= (count vals) (test :batch-size))
                                            (conj vals op)
                                            [(promise) op]))
          [[prevpromise & prevlist]
           [newpromise  & newlist]] (swap-vals! syncatom maybe-conj opval)]
      (when (< (count newlist) (count prevlist))
        ;; maybe-conj created a new batch for our operation, so we need to dispatch the previous
        (->> (do-batch-insert test @bucket prevlist)
             (deliver prevpromise)))
      (when (= 1 (count newlist))
        ;; If we are the first op, and after 100ms the batch still isn't dispatched, dispatch
        ;; it regardless of whether it is full. 
        (if (not (deref newpromise 100 nil))
            (let [curr-syncatom @syncatom]
                 (if (and (= newpromise (first curr-syncatom))
                          (compare-and-set! syncatom curr-syncatom [(promise)]))
                     (->> (do-batch-insert test @bucket (rest curr-syncatom))
                          (deliver newpromise))))))
      (-> (deref newpromise)
          (get (.id opval)))))
      
  (maybe-close [this]
    (when (= (swap! status dec) 0)
      (->> @bucket
           (.close)
           (.toBlocking)
           (.single))
      (.disconnect @cluster))))


;; Using multiple couchbase java sdk clients is necessary to expose some race
;; conditions, so we want an easy way to dispatch operations across a variable
;; number of clients. We create a pool of clojure client wrappers and distribute
;; ops across them randomly
(defrecord ClientPool [status pool gen-client]
  CouchbaseClient
  (maybe-setup [this test]
    (if (compare-and-set! status nil :preparing)
      (let [clients (vec (repeatedly (test :pool-size) gen-client))]
        (doseq [client clients] (maybe-setup client test))
        (reset! pool clients)
        (reset! status 0))
      (while (= :preparing @status) (Thread/sleep 100)))
    (swap! status inc))

  (invoke [this test optype opval]
    (invoke (rand-nth @pool) test optype opval))

  (maybe-close [this]
    (if (= (swap! status dec) 0)
      (doseq [client @pool]
        (maybe-close client)))))



;; DCP wrapper

(defn dcpControlEventHandler
  [client store idle]
  (let [rollback-event (fn [event]
                         (let [descr (RollbackMessage/toString event)
                               vbid  (RollbackMessage/vbucket event)
                               seqno (RollbackMessage/seqno   event)
                               oksub     (reify rx.functions.Action0
                                           (call [_] (info descr "completed ok")))
                               errorsub  (reify rx.functions.Action1
                                           (call [_ e] (error descr "failed with error: " e)
                                                       (reset! store :INVALID)
                                                       (throw e)))]
                           (info "DCPControlEventHandler got:" descr)
                           (if-not (= @store :INVALID)
                             (swap! store (partial remove #(and (= (:vbucket %) vbid) (> (:seqno %) seqno))))
                             (throw (RuntimeException. "Store state invalid")))
                           (-> (.rollbackAndRestartStream client vbid seqno)
                               (.subscribe oksub errorsub))))]
    (reify ControlEventHandler
      (onEvent [_ flowController event]
        (swap! idle min 0)
        (cond
          (DcpSnapshotMarkerRequest/is event) nil
          (RollbackMessage/is event)  (rollback-event event)
          :else                       (do (reset! store :INVALID)
                                          (error "DCP client got unknown control event")
                                          (throw (RuntimeException. "Unknown DCP control event: " event))))
        (.ack flowController event)
        (.release event)))))

(defn dcpDataEventHandler
  [store idle slow]
  (let [add-event (fn [event]
                    (let [key   (DcpMutationMessage/keyString event)
                          vbid  (DcpMutationMessage/partition event)
                          seqno (DcpMutationMessage/bySeqno   event)]
                      (if-not (= @store :INVALID)
                        (swap! store conj {:key key :seqno seqno :key-status :exists :vbucket vbid})
                        (throw (RuntimeException. "Store state invalid")))))
        del-event (fn [event]
                    (let [key   (DcpDeletionMessage/keyString event)
                          vbid  (DcpDeletionMessage/partition event)
                          seqno (DcpDeletionMessage/bySeqno   event)]
                      (if-not (= @store :INVALID)
                        (swap! store conj {:key key :seqno seqno :key-status :deleted :vbucket vbid})
                        (throw (RuntimeException. "Store state invalid")))))]

    (reify DataEventHandler
      (onEvent [_ flowController event]
        (if @slow (Thread/sleep 30))
        (swap! idle min 0)
        (cond
          (DcpMutationMessage/is event)   (add-event event)
          (DcpDeletionMessage/is event)   (del-event event)
          (DcpExpirationMessage/is event) (del-event event)
          :else                           (do
                                            (reset! store :INVALID)
                                            (error "GOT UNKNOWN DCP DATA EVENT:" event)
                                            (throw (RuntimeException. "Unknown DCP data event: " event))))
        (.ack flowController event)
        (.release event)))))

;; Open a dcp stream and keep track of all existing keys in a bucket. We use an
;; atom containing a sequence of maps to track mutations. This lets us perform
;; rollbacks by filtering the invalidated mutations out of the sequence. When
;; we receive the read op from jepsen we discard all but the last mutation for a
;; given key, and use that to determine if the key exists.
(defrecord DcpClient2 [status client store idle slow]
  CouchbaseClient
  (maybe-setup [this test]
    (if (compare-and-set! status nil :preparing)
      (let [server-version (util/get-version (first (test :nodes)))
            new-client     (if (>= (first server-version) 5)
                               (-> (Client/configure)
                                   (.hostnames (test :nodes))
                                   (.username "Administrator")
                                   (.password "abc123")
                                   (.controlParam DcpControl$Names/SUPPORTS_CURSOR_DROPPING true)
                                   (.controlParam DcpControl$Names/CONNECTION_BUFFER_SIZE 10000)
                                   (.bufferAckWatermark 75)
                                   (.bucket "default")
                                   (.build))
                               (-> (Client/configure)
                                   (.hostnames (test :nodes))
                                   (.controlParam DcpControl$Names/SUPPORTS_CURSOR_DROPPING true)
                                   (.controlParam DcpControl$Names/CONNECTION_BUFFER_SIZE 10000)
                                   (.bufferAckWatermark 75)
                                   (.bucket "default")
                                   (.build)))]
        (reset! client new-client)
        (reset! store nil)
        (reset! idle 0)
        (reset! slow false)
        (.controlEventHandler @client (dcpControlEventHandler @client store idle))
        (.dataEventHandler @client (dcpDataEventHandler store idle slow))
        (-> @client (.connect) (.await))
        (-> @client (.initializeState StreamFrom/BEGINNING StreamTo/INFINITY) (.await))
        (-> @client (.startStreaming (make-array Short 0)) (.await))
        (reset! status 0))
      (while (= :preparing @status) (Thread/sleep 100)))
    (swap! status inc))

  (invoke [this test optype op]
    (assert (= optype :get-all-keys))
    (when @slow
      (info "Making DCP client fast again")
      (reset! slow false))
    (while (< @idle 10)
      (Thread/sleep 100)
      (swap! idle inc))
    (info "Finished getting mutations, parsing result...")
    (if-not (= @store :INVALID)
      (->> (group-by :key @store)
           (vals)
           (map #(apply max-key :seqno %))
           (keep #(if (= (:key-status %) :exists) (:key %))))
      (throw (RuntimeException. "Store state invalid"))))

  (maybe-close [this]
    (if (= (swap! status dec) 0)
      (-> @client (.disconnect) (.await)))))

(defn basic-client []
  (BasicClient. (atom nil) (atom nil) (atom nil)))

(defn batch-insert []
  (BatchInsertClient. (atom nil) (atom nil) (atom nil) (atom nil)))

(defn batch-insert-pool []
  (ClientPool. (atom nil) (atom nil) batch-insert))

(defn dcp-client []
  (DcpClient2. (atom nil) (atom nil) (atom nil) (atom nil) (atom nil)))

;; This simple client first opens the actual dcp stream just before the read, ie
;; once we've finished performing ops against the cluster. This lets us
;; differentiate between issues that occur with a single dcp client and issues
;; affecting the entire clusters
(defn simple-dcp-client []
  (reify CouchbaseClient
    (maybe-setup [this test] this)
    (invoke [this test optype op]
      (let [real-client (dcp-client)
            _           (maybe-setup real-client test)
            result      (invoke real-client test optype op)
            _           (maybe-close real-client)]
        result))
    (maybe-close [this])))
