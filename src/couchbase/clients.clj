(ns couchbase.clients
  (:require [clojure.tools.logging :refer :all]
            [couchbase.cbclients :as cbclients]
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
           com.couchbase.client.core.error.DurabilityAmbiguous
           com.couchbase.client.core.error.DurabilityImpossibleException
           com.couchbase.client.core.error.DurabilityLevelNotAvailableException
           com.couchbase.client.core.error.DurableWriteInProgressException
           com.couchbase.client.core.error.RequestTimeoutException
           com.couchbase.client.core.error.TemporaryFailureException
           com.couchbase.client.core.msg.kv.DurabilityLevel))

;; Helper functions to workaround JDK-4283544

(defn apply-durability-options!
  "Helper function to apply durability level to perform sync-writes"
  [mutation-options op]
  (if-let [level (case (op :durability-level 0)
                   0 nil
                   1 DurabilityLevel/MAJORITY
                   2 DurabilityLevel/MAJORITY_AND_PERSIST_ON_MASTER
                   3 DurabilityLevel/PERSIST_TO_MAJORITY)]
    ;; Due to java bug JDK-4283544 we can't call withDurabilityLevel from clojure
    ;; in the usual fashion, we need to use the following workaround:
    (let [parameters (into-array java.lang.Class [DurabilityLevel])
          method     (.getDeclaredMethod CommonDurabilityOptions "withDurabilityLevel" parameters)
          arguments  (into-array java.lang.Object [level])]
      (.setAccessible method true)
      (.invoke method mutation-options arguments))))

(defn apply-observe-options!
  "Helper function to apply the old observe based replicate-to/persist-to"
  [mutation-options op]
  (let [replicate-to (case (op :replicate-to 0)
                       0 nil
                       1 ReplicateTo/ONE
                       2 ReplicateTo/TWO
                       3 ReplicateTo/THREE)
        persist-to   (case (op :persist-to 0)
                       0 nil
                       1 PersistTo/ONE
                       2 PersistTo/TWO
                       3 PersistTo/THREE)]
    (when (or replicate-to persist-to)
        ;; Again due to java bug JDK-4283544 we can't just call withDurability
        ;; in the usual fashion, we need the following workaround:
      (let [parameters (into-array java.lang.Class [PersistTo ReplicateTo])
            method (.getDeclaredMethod CommonDurabilityOptions "withDurability" parameters)
            arguments (into-array java.lang.Object [persist-to replicate-to])]
        (.setAccessible method true)
        (.invoke method mutation-options arguments)))))

;; ===============
;; Register Client
;; ===============

(defn do-register-read [collection op]
  (assert (= (:f op) :read))
  (let [[rawkey _] (:value op)
        dockey (format "jepsen%04d" rawkey)]
    (try
      (if-let [get-result (-> (.get collection dockey) (.orElse nil))]
        (assoc op
               :type :ok
               :cas (.cas get-result)
               :value (->> (.contentAs get-result Integer)
                           (independent/tuple rawkey)))
        (assoc op
               :type :ok
               :value (independent/tuple rawkey :nil)))
      ;; Reads are idempotent, so it's ok to just :fail on any exception
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
      (catch DurabilityAmbiguous _
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
      (let [get-current (-> (.get collection dockey) (.get))
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
      (catch DurabilityAmbiguous _
        (assoc op :type :info, :error :SyncWriteAmbiguous))
      (catch RequestTimeoutException _
        (assoc op :type :info, :error :Timeout))
      (catch CouchbaseException e
        (assoc op :type :info, :error e)))))

(defrecord NewRegisterClient [cluster bucket collection]
  client/Client
  (open! [this test node]
    (merge this (cbclients/get-client-from-pool test)))

  (setup! [_ _])
  (invoke! [_ test op]
    (case (:f op)
      :read (do-register-read collection op)
      :write (do-register-write collection op)
      :cas (do-register-cas collection op)))

  (close! [_ _])

  (teardown! [_ test]
    (cbclients/shutdown-pool test)))

;; Wrapper as records aren't externally visible
(defn register-client []
  (NewRegisterClient. nil nil nil))

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
          token  (-> (.mutationToken result) (.orElse nil))]
      (assoc op
             :type :ok
             :mutation-token (.toString token)))
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
    (catch DurabilityAmbiguous _
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
             :mutation-token (.toString token)))
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
    (catch DurabilityAmbiguous _
      (assoc op :type :info, :error :SyncWriteAmbiguous))
    (catch RequestTimeoutException _
      (assoc op :type :info, :error :Timeout))
    (catch CouchbaseException e
      (assoc op :type :info, :error e))))

(defrecord NewSetClient [cluster bucket collection dcpclient]
  client/Client
  (open! [this test node]
    (merge this (cbclients/get-client-from-pool test)))

  (setup! [_ _])
  (invoke! [_ test op]
    (case (:f op)
      :add (do-set-add collection op)
      :del (do-set-del collection op)

      :read (->> (cbclients/get-all-keys dcpclient test)
                 (map #(Integer/parseInt (subs % 6)))
                 (sort)
                 (assoc op :type :ok, :value))

      :dcp-start-streaming (do (cbclients/start-streaming dcpclient test) op)))
  (close! [_ _])
  (teardown! [_ test]
    (cbclients/shutdown-pool test)))

(defn set-client [dcpclient]
  (NewSetClient. nil nil nil dcpclient))
