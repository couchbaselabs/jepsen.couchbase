(ns couchbase.clients
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase.cbclients :as cbclients]
            [dom-top.core :refer [with-retry]]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [slingshot.slingshot :refer [try+]])
  (:import com.couchbase.client.java.kv.CommonDurabilityOptions
           com.couchbase.client.java.kv.PersistTo
           com.couchbase.client.java.kv.ReplicateTo
           com.couchbase.client.java.kv.InsertOptions
           com.couchbase.client.java.kv.RemoveOptions
           com.couchbase.client.java.kv.ReplaceOptions
           com.couchbase.client.java.kv.UpsertOptions
           com.couchbase.client.core.error.CASMismatchException
           com.couchbase.client.core.error.CouchbaseException
           com.couchbase.client.core.error.DurabilityAmbiguousException
           com.couchbase.client.core.error.DurabilityImpossibleException
           com.couchbase.client.core.error.DurabilityLevelNotAvailableException
           com.couchbase.client.core.error.DurableWriteInProgressException
           com.couchbase.client.core.error.KeyNotFoundException
           com.couchbase.client.core.error.RequestTimeoutException
           com.couchbase.client.core.error.TemporaryFailureException
           com.couchbase.client.core.msg.kv.DurabilityLevel
           com.couchbase.transactions.Transactions
           com.couchbase.transactions.TransactionJsonDocument
           com.couchbase.transactions.error.TransactionFailed
           com.couchbase.transactions.config.PerTransactionConfig
           com.couchbase.transactions.config.PerTransactionConfigBuilder
           com.couchbase.transactions.TransactionDurabilityLevel
           java.util.function.Consumer))

;; Helper functions to apply durability options

(defn apply-durability-options!
  "Helper function to apply durability level to perform sync-writes"
  [mutation-options op]
  (if-let [level (case (op :durability-level 0)
                   0 nil
                   1 DurabilityLevel/MAJORITY
                   2 DurabilityLevel/MAJORITY_AND_PERSIST_ON_MASTER
                   3 DurabilityLevel/PERSIST_TO_MAJORITY)]
    (.durability mutation-options level)))

(defn apply-observe-options!
  "Helper function to apply the old observe based replicate-to/persist-to"
  [mutation-options op]
  (when (or (pos? (op :replicate-to 0))
            (pos? (op :persist-to 0)))
    (let [replicate-to (case (op :replicate-to 0)
                         0 ReplicateTo/NONE
                         1 ReplicateTo/ONE
                         2 ReplicateTo/TWO
                         3 ReplicateTo/THREE)
          persist-to   (case (op :persist-to 0)
                         0 PersistTo/NONE
                         1 PersistTo/ONE
                         2 PersistTo/TWO
                         3 PersistTo/THREE)]
      (.durability mutation-options persist-to replicate-to))))

;; ===============
;; Register Client
;; ===============

(defn do-register-read [collection op]
  (assert (= (:f op) :read))
  (let [[rawkey _] (:value op)
        dockey (format "jepsen%04d" rawkey)]
    (try
      (let [get-result (.get collection dockey)]
        (assoc op
               :type :ok
               :cas (.cas get-result)
               :value (independent/tuple rawkey (.contentAs get-result Integer))))
      (catch KeyNotFoundException _
        (assoc op :type :ok :value (independent/tuple rawkey :nil)))
      ;; Reads are idempotent, so it's ok to just :fail on any exception. Note
      ;; that we don't :fail on a KeyNotFoundException, since translating between
      ;; the Couchbase and Jepsen models we know the read succeeded, but it wouldn't
      ;; strictly be wrong if we did return it as a failure (i.e it wouldn't cause
      ;; false-positive linearizability errors to be detected; it might increase the
      ;; probability of a linearizability error going undetected, but Jepsen can't
      ;; prove correctness anyway.
      (catch RequestTimeoutException _
        (assoc op :type :fail, :error :Timeout))
      (catch TemporaryFailureException _
        (assoc op :type :fail, :error :Etmpfail))
      (catch CouchbaseException e
        (assoc op :type :fail, :error e)))))

(defn do-register-write [collection op]
  (assert (= (:f op) :write))
  (let [[rawkey opval] (:value op)
        dockey (format "jepsen%04d" rawkey)]
    (try
      (let [opts (doto (UpsertOptions/upsertOptions)
                   (apply-durability-options! op)
                   (apply-observe-options! op))
            upsert-result (.upsert collection dockey opval opts)
            mutation-token (.mutationToken upsert-result)]
        (assoc op
               :type :ok
               :cas (.cas upsert-result)
               :mutation-token (-> mutation-token (.orElse nil) (str))))
      ;; Certain failures - we know the operations did not take effect
      (catch DurabilityImpossibleException _
        (assoc op :type :fail, :error :DurabilityImpossible))
      (catch DurabilityLevelNotAvailableException _
        (assoc op :type :fail, :error :DurabilityLevelNotAvailable))
      (catch DurableWriteInProgressException _
        (assoc op :type :fail, :error :SyncWriteInProgress))
      (catch TemporaryFailureException _
        (assoc op :type :fail, :error :Etmpfail))
      ;; Ambiguous result - operation may or may not take effect
      (catch DurabilityAmbiguousException _
        (assoc op :type :info, :error :SyncWriteAmbiguous))
      (catch RequestTimeoutException _
        (assoc op :type :info, :error :Timeout))
      (catch CouchbaseException e
        (assoc op :type :info, :error e)))))

(defn do-register-cas [collection op]
  (assert (= (:f op) :cas))
  (let [[rawkey [swap-from swap-to]] (:value op)
        dockey (format "jepsen%04d" rawkey)]
    (try
      (let [get-current (.get (.get collection dockey))
            current-value (.contentAs get-current Integer)
            current-cas (.cas get-current)]
        (if (= current-value swap-from)
          (let [opts (doto (ReplaceOptions/replaceOptions)
                       (.cas current-cas)
                       (apply-durability-options! op)
                       (apply-observe-options! op))
                replace-result (.replace collection dockey swap-to opts)
                mutation-token (.mutationToken replace-result)]
            (assoc op
                   :type :ok
                   :cas (.cas replace-result)
                   :mutation-token (-> mutation-token (.orElse nil) (str))))
          (assoc op :type :fail :error :ValueNotSwapFrom)))
      ;; Certain failures - we know the operations did not take effect
      (catch java.util.NoSuchElementException _
        (assoc op :type :fail, :error :GetFailed))
      (catch CASMismatchException _
        (assoc op :type :fail, :error :CasMismatch))
      (catch DurabilityImpossibleException _
        (assoc op :type :fail, :error :DurabilityImpossible))
      (catch DurabilityLevelNotAvailableException _
        (assoc op :type :fail, :error :DurabilityLevelNotAvailable))
      (catch DurableWriteInProgressException _
        (assoc op :type :fail, :error :SyncWriteInProgress))
      (catch TemporaryFailureException _
        (assoc op :type :fail, :error :Etmpfail))
      ;; Ambiguous result - operation may or may not take effect
      (catch DurabilityAmbiguousException _
        (assoc op :type :info, :error :SyncWriteAmbiguous))
      (catch RequestTimeoutException _
        (assoc op :type :info, :error :Timeout))
      (catch CouchbaseException e
        (assoc op :type :info, :error e)))))

(defn per-txn-config [op]
  (if-let [durability-level (case (op :durability-level 0)
                              0 nil
                              1 TransactionDurabilityLevel/MAJORITY
                              2 TransactionDurabilityLevel/MAJORITY_AND_PERSIST_ON_MASTER
                              3 TransactionDurabilityLevel/PERSIST_TO_MAJORITY)]
    (-> (PerTransactionConfigBuilder/create)
        (.durabilityLevel durability-level)
        (.build))
    (.build (PerTransactionConfigBuilder/create))))

(defn ^Consumer f-to-consumer [f]
  "Converts a function to java.util.function.Consumer."
  (reify Consumer
    (accept [this arg] (f arg))))

(defn do-register-txn [collection txn op]
  (assert (= (:f op) :txn))
  (try
    (let [coll (atom collection)
          coll-index (first (:value op))
          op-attempt-history (atom [])
          opvals (atom (second (:value op)))]
      (.run
       txn
       (f-to-consumer
        (fn [ctx]
          (doseq [op @opvals]
            (let [[optype rawkey opval] op
                  dockey (format "jepsen%04d" rawkey)]
              (case optype
                :read
                (let [get-result (.orElse (.get ctx @coll dockey) nil)
                      get-value (if (nil? get-result) :nil (.contentAs get-result Integer))]
                  (reset! op-attempt-history (conj @op-attempt-history [optype rawkey get-value])))
                :write
                (let [get-result (.orElse (.get ctx @coll dockey) nil)]
                  (if (nil? get-result)
                    (.insert ctx @coll dockey opval)
                    (.replace ctx get-result opval))
                  (reset! op-attempt-history (conj @op-attempt-history [optype rawkey opval])))))))) (per-txn-config op))
      (assoc op :type :ok,
             :value (independent/tuple coll-index (take-last (count @opvals) @op-attempt-history))
             :attempt-history @op-attempt-history))
      ;; Certain failures - we know the operations did not take effect
    (catch TransactionFailed e
      (assoc op :type :fail, :error (str e), :msg (str e)))
    (catch DurabilityImpossibleException _
      (assoc op :type :fail, :error :DurabilityImpossible))
    (catch DurabilityLevelNotAvailableException _
      (assoc op :type :fail, :error :DurabilityLevelNotAvailable))
    (catch DurableWriteInProgressException _
      (assoc op :type :fail, :error :SyncWriteInProgress))
    (catch TemporaryFailureException _
      (assoc op :type :fail, :error :Etmpfail))
    ;; Ambiguous result - operation may or may not take effect
    (catch DurabilityAmbiguousException _
      (assoc op :type :info, :error :SyncWriteAmbiguous))
    (catch RequestTimeoutException _
      (assoc op :type :info, :error :Timeout))
    (catch CouchbaseException e
      (assoc op :type :info, :error e))))

(defrecord NewRegisterClient [cluster bucket collection env txn]
  client/Client
  (open! [this test node]
    (merge this (cbclients/get-client-from-pool test)))

  (setup! [_ _])
  (invoke! [_ test op]
    (case (:f op)
      :read (do-register-read collection op)
      :write (do-register-write collection op)
      :cas (do-register-cas collection op)
      :txn (do-register-txn collection txn op)))

  (close! [_ _])

  (teardown! [_ test]
    (cbclients/shutdown-pool test)))

;; Wrapper as records aren't externally visible
(defn register-client []
  (NewRegisterClient. nil nil nil nil nil))

;; ==========
;; Set Client
;; ==========

(defn do-set-add [collection op]
  (try
    (let [opts (doto (InsertOptions/insertOptions)
                 (apply-durability-options! op)
                 (apply-observe-options! op))
          dockey (format "jepsen%010d" (:value op))
          result (.insert collection dockey (:value op) opts)
          token  (.orElse (.mutationToken result) nil)]
      (assoc op
             :type :ok
             :mutation-token (str token)))
    ;; Certain failures - we know the operations did not take effect
    (catch DurabilityImpossibleException _
      (assoc op :type :fail, :error :DurabilityImpossible))
    (catch DurabilityLevelNotAvailableException _
      (assoc op :type :fail, :error :DurabilityLevelNotAvailable))
    (catch DurableWriteInProgressException _
      (assoc op :type :fail, :error :SyncWriteInProgress))
    (catch TemporaryFailureException _
      (assoc op :type :fail :error :Etmpfail))
    ;; Ambiguous result - operation may or may not take effect
    (catch DurabilityAmbiguousException _
      (assoc op :type :info, :error :SyncWriteAmbiguous))
    (catch RequestTimeoutException _
      (assoc op :type :info, :error :Timeout))
    (catch CouchbaseException e
      (assoc op :type :info, :error e))))

(defn do-set-del [collection op]
  (try
    (let [opts (doto (RemoveOptions/removeOptions)
                 (apply-durability-options! op)
                 (apply-observe-options! op))
          dockey (format "jepsen%010d" (:value op))
          result (.remove collection dockey opts)
          token  (.mutationToken result)]
      (assoc op
             :type :ok
             :mutation-token (str token)))
     ;; Certain failures - we know the operations did not take effect
    (catch DurabilityImpossibleException _
      (assoc op :type :fail, :error :DurabilityImpossible))
    (catch DurabilityLevelNotAvailableException _
      (assoc op :type :fail, :error :DurabilityLevelNotAvailableException))
    (catch DurableWriteInProgressException _
      (assoc op :type :fail, :error :SyncWriteInProgress))
    (catch TemporaryFailureException _
      (assoc op :type :fail, :error :Etmpfail))
    ;; Ambiguous result - operation may or may not take effect
    (catch DurabilityAmbiguousException _
      (assoc op :type :info, :error :SyncWriteAmbiguous))
    (catch RequestTimeoutException _
      (assoc op :type :info, :error :Timeout))
    (catch CouchbaseException e
      (assoc op :type :info, :error e))))

(defn check-if-exists [collection rawkey]
  (with-retry [attempts 120]
    (let [key (format "jepsen%010d" rawkey)
          get (.get collection key)]
      ;; If the key is found, return it
      rawkey)
    ;; Else if we get a KeyNotFoundException, return nil
    (catch KeyNotFoundException _ nil)
    ;; Retry other failures, throwing an exception if it persists
    (catch Exception e
      (if (pos? attempts)
        (do (Thread/sleep 1000)
            (retry (dec attempts)))
        (do (warn "Couldn't read key" rawkey)
            (throw e))))))

(defrecord NewSetClient [cluster bucket collection dcpclient]
  client/Client
  (open! [this test node]
    (merge this (cbclients/get-client-from-pool test)))

  (setup! [_ _])
  (invoke! [_ test op]
    (case (:f op)
      :add (do-set-add collection op)
      :del (do-set-del collection op)

      :read (if dcpclient
              (->> (cbclients/get-all-keys dcpclient test)
                   (map #(Integer/parseInt (subs % 6)))
                   (sort)
                   (assoc op :type :ok, :value))
              (try
                (->> @(:history test)
                     (filter #(and (= (:type %) :invoke)))
                     (apply max-key #(or (:value %) -1))
                     (:value)
                     (inc)
                     (range)
                     (pmap #(check-if-exists collection %))
                     (filter some?)
                     (doall)
                     (assoc op :type :ok, :value))
                (catch CouchbaseException e
                  (warn "Encountered errors reading some keys, keys might be stuck pending?")
                  (assoc op :type :fail, :error e))))

      :dcp-start-streaming (do (cbclients/start-streaming dcpclient test) op)))
  (close! [_ _])
  (teardown! [_ test]
    (cbclients/shutdown-pool test)))

(defn set-client [dcpclient]
  (NewSetClient. nil nil nil dcpclient))
