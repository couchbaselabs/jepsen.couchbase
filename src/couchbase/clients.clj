(ns couchbase.clients
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase.cbclients :as cbclients]
            [couchbase.clients-utils :as clientUtils]
            [couchbase.cbjcli :as cbjcli]
            [dom-top.core :as domTop]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [slingshot.slingshot :refer [try+]])
  (:import (com.couchbase.client.java.kv MutateInResult
                                         GetResult
                                         LookupInResult
                                         MutationResult
                                         ReplaceOptions
                                         InsertOptions
                                         RemoveOptions
                                         IncrementOptions
                                         DecrementOptions
                                         CounterResult)
           (com.couchbase.client.core.error TemporaryFailureException
                                            CouchbaseException
                                            DurabilityImpossibleException
                                            DurabilityLevelNotAvailableException
                                            DurableWriteInProgressException
                                            RequestCanceledException
                                            DurabilityAmbiguousException
                                            DocumentNotFoundException
                                            CasMismatchException
                                            ServerOutOfMemoryException
                                            DocumentExistsException
                                            TimeoutException
                                            UnambiguousTimeoutException
                                            AmbiguousTimeoutException)
           (com.couchbase.client.java Collection
                                      BinaryCollection)
           (java.util NoSuchElementException)
           (com.couchbase.client.core.msg.kv MutationToken))
  (:gen-class))

(defmacro try-read-with-exception->op
  [op & body]
  `(try
     ~@body
     (catch UnambiguousTimeoutException e#
       (assoc ~op :type :fail, :error :UnambiguousTimeoutException :msg (.getMessage e#)))
     (catch TemporaryFailureException e#
       (assoc ~op :type :fail, :error :Etmpfail :msg (.getMessage e#)))
     (catch ServerOutOfMemoryException e#
       (assoc ~op :type :fail :error :ServerOutOfMemoryException))
     (catch CouchbaseException e#
       (assoc ~op :type :fail, :error e#))))

(defmacro try-write-with-exception->op
  [op & body]
  `(try
     ~@body
      ;; Certain failures - we know the operations did not take effect
     (catch DurabilityImpossibleException e#
       (assoc ~op :type :fail, :error :DurabilityImpossible :msg (.getMessage e#)))
     (catch DurabilityLevelNotAvailableException e#
       (assoc ~op :type :fail, :error :DurabilityLevelNotAvailable :msg (.getMessage e#)))
     (catch DurableWriteInProgressException e#
       (assoc ~op :type :fail, :error :SyncWriteInProgress :msg (.getMessage e#)))
     (catch TemporaryFailureException e#
       (assoc ~op :type :fail, :error :Etmpfail :msg (.getMessage e#)))
     (catch ServerOutOfMemoryException e#
       (assoc ~op :type :fail :error :ServerOutOfMemoryException))
     (catch DocumentExistsException e#
       (assoc ~op :type :fail :error :DocumentExistsException))
     (catch CasMismatchException e#
       (assoc ~op :type :fail, :error :CASMismatchException))
     (catch UnambiguousTimeoutException e#
       (assoc ~op :type :fail, :error :UnambiguousTimeoutException :msg (.getMessage e#)))
      ;; Ambiguous result - operation may or may not take effect
     (catch RequestCanceledException e#
       (assoc ~op :type :info :error :RequestCanceledException :msg (.getMessage e#)))
     (catch DurabilityAmbiguousException e#
       (assoc ~op :type :info, :error :SyncWriteAmbiguous :msg (.getMessage e#)))
     (catch TimeoutException e#
       (assoc ~op :type :info, :error :RequestTimeoutException :msg (.getMessage e#)))
     (catch CouchbaseException e#
       (assoc ~op :type :info, :error e#))))

;; ===============
;; Register Client
;; ===============

(defn do-register-read [collection op]
  (assert (= (:f op) :read))
  (let [[rawKey _] (:value op)
        docKey (format "jepsen%04d" rawKey)]
    (try-read-with-exception->op
     op
     (let [get-obj (if-not cbjcli/*use-subdoc*
                     (.get ^Collection collection docKey)
                     (clientUtils/perform-subdoc-get collection docKey))]
       (if-not cbjcli/*use-subdoc*
         (assoc op
                :type :ok
                :cas (.cas ^GetResult get-obj)
                :value (independent/tuple rawKey ^Integer (clientUtils/get-int-from-get-result get-obj)))
         (assoc op
                :type :ok
                :cas (.cas ^LookupInResult get-obj)
                :value (independent/tuple rawKey ^Integer (clientUtils/get-int-from-look-up-obj get-obj)))))
      ;; Note we don't :fail on a DocumentNotFoundException, since translating
      ;; between the Couchbase and Jepsen models we know the read succeeded,
      ;; but it wouldn't strictly be wrong if we did return it as a failure
      ;; (i.e it wouldn't cause false-positive linearizability errors to be
      ;; detected; it might increase the probability of a linearizability error
      ;; going undetected, but Jepsen can't prove correctness anyway.
     (catch DocumentNotFoundException _
       (assoc op :type :ok :value (independent/tuple rawKey :nil))))))

(defn do-register-write [collection op]
  (assert (= (:f op) :write))
  (let [[rawKey opVal] (:value op)
        docKey (format "jepsen%04d" rawKey)]
    (try-write-with-exception->op
     op
     (let [mutation-option (select-keys op [:durability-level :replicate-to :persist-to])
           upsert-result (if-not cbjcli/*use-subdoc*
                           (.upsert ^Collection collection
                                    docKey
                                    (clientUtils/create-int-json-obj opVal)
                                    (clientUtils/get-upsert-ops op))
                           (clientUtils/perform-subdoc-upsert ^Collection collection
                                                              docKey
                                                              opVal
                                                              mutation-option))]
       (if-not cbjcli/*use-subdoc*
         (assoc op
                :type :ok
                :cas (.cas ^MutationResult upsert-result)
                :mutation-token (str (.mutationToken ^MutationResult upsert-result)))
         (assoc op
                :type :ok
                :cas (.cas ^MutateInResult upsert-result)))))))

(defn do-register-cas [collection op]
  (assert (= (:f op) :cas))
  (let [[rawkey [swap-from swap-to]] (:value op)
        docKey (format "jepsen%04d" rawkey)]
    (try-write-with-exception->op
     op
     (let [get-current ^GetResult (.get ^Collection collection docKey)
           current-value (clientUtils/get-int-from-get-result get-current)
           current-cas (.cas get-current)]
       (if (= current-value swap-from)
         (let [replace-result (.replace ^Collection collection
                                        ^String docKey
                                        (clientUtils/create-int-json-obj swap-to)
                                        ^ReplaceOptions (clientUtils/get-replace-ops op current-cas))
               mutation-token (.mutationToken ^MutationResult replace-result)]
           (assoc op
                  :type :ok
                  :cas (.cas replace-result)
                  :mutation-token (str mutation-token)))
         (assoc op :type :fail :error :ValueNotSwapFrom :curr-cas current-cas :curr-value current-value)))
     (catch NoSuchElementException e
       (assoc op :type :fail, :error :GetFailed :msg (.getMessage e)))
     (catch DocumentNotFoundException _
       (assoc op :type :fail :error :DocumentNotFoundException)))))

(defrecord NewRegisterClient [cluster bucket collection env]
  client/Client
  (open! [this testData node]
    (merge this (cbclients/get-client-from-pool testData)))

  (setup! [_ _])
  (invoke! [_ testData op]
    (case (:f op)
      :read (do-register-read collection op)
      :write (do-register-write collection op)
      :cas (do-register-cas collection op)))

  (close! [_ _])

  (teardown! [_ testData]
    (cbclients/shutdown-pool testData)))

;; Wrapper as records aren't externally visible
(defn register-client []
  (NewRegisterClient. nil nil nil nil))

;; ==========
;; Set Client
;; ==========

(defn do-set-add [collection op]
  (try-write-with-exception->op
   op
   (let [docKey (format "jepsen%010d" (:value op))
         result (.insert ^Collection collection
                         ^String docKey
                         (clientUtils/create-int-json-obj (:value op))
                         ^InsertOptions (clientUtils/get-insert-ops op))
         token  (.orElse (.mutationToken ^MutationResult result) nil)]
     (assoc op
            :type :ok
            :mutation-token (str token)))))

(defn do-set-del [collection op]
  (try-write-with-exception->op
   op
   (let [docKey (format "jepsen%010d" (:value op))
         result (.remove ^Collection collection
                         ^String docKey
                         ^RemoveOptions (clientUtils/get-remove-ops op))
         token  (.mutationToken ^MutationResult result)]
     (assoc op
            :type :ok
            :mutation-token (str ^MutationToken token)))))

(defn check-if-exists [collection rawKey]
  (domTop/with-retry [attempts 120]
    (let [key (format "jepsen%010d" rawKey)]
      (if (.get ^Collection collection key)
      ;; If the key is found, return it
        rawKey))
    ;; Else if we get a DocumentNotFoundException, return nil
    (catch DocumentNotFoundException _ nil)
    ;; Retry other failures, throwing an exception if it persists
    (catch Exception e
      (if (pos? attempts)
        (do (Thread/sleep 1000)
            (retry (dec attempts)))
        (do (warn "Couldn't read key" rawKey)
            (throw e))))))

(defn do-set-read [collection op dcpClient testData]
  (if dcpClient
    (->> (cbclients/get-all-keys dcpClient testData)
         (map #(Integer/parseInt (subs % 6)))
         (sort)
         (assoc op :type :ok, :value))
    (try
      (->> (or (:value op)
               (->> @(:history testData)
                    (filter #(= (:type %) :invoke))
                    (apply max-key #(or (:value %) -1))
                    (:value)))
           (inc)
           (range)
           (pmap #(check-if-exists collection %))
           (filter some?)
           (doall)
           (assoc op :type :ok, :value))
      (catch CouchbaseException e
        (warn "Encountered errors reading some keys, keys might be stuck pending?")
        (assoc op :type :fail, :error e)))))

(defn do-counter-add [collection op]
  (try-write-with-exception->op
   op
   (let [docKey (str "jepsen")
         binaryCollection ^BinaryCollection (.binary collection)
         result  (if (pos? (:value op))
                   (.increment ^BinaryCollection binaryCollection
                               ^String docKey
                               ^IncrementOptions (clientUtils/get-increment-ops op))
                   (.decrement ^BinaryCollection binaryCollection
                               ^String docKey
                               ^DecrementOptions (clientUtils/get-decrement-ops op)))

         token  (.orElse (.mutationToken ^CounterResult result) nil)]
     (assoc op
            :type :ok
            :cas (.cas ^CounterResult result)
            :mutation-token (str ^MutationToken token)
            :current-value (.content result)))))

(defn do-counter-read [collection op]
  (assert (= (:f op) :read))
  (let [docKey (str "jepsen")]
    (try-read-with-exception->op
     op
     (let [get-obj (.increment ^BinaryCollection (.binary collection)
                               ^String docKey
                               (.delta ^IncrementOptions (clientUtils/get-increment-ops op) 0))
           token  (.orElse (.mutationToken ^CounterResult get-obj) nil)]
       (assoc op
              :type :ok
              :cas (.cas ^CounterResult get-obj)
              :mutation-token (str ^MutationToken token)
              :value ^Integer (.content ^CounterResult get-obj)))
     (catch DocumentNotFoundException _
       (assoc op :type :ok :value :nil)))))

(defrecord NewSetClient [cluster bucket collection dcpClient]
  client/Client
  (open! [this testData node]
    (merge this (cbclients/get-client-from-pool testData)))

  (setup! [_ _])
  (invoke! [_ testData op]
    (case (:f op)
      :add (do-set-add collection op)
      :del (do-set-del collection op)
      :read (do-set-read collection op dcpClient testData)

      :dcp-start-streaming (do (cbclients/start-streaming dcpClient testData) op)))
  (close! [_ _])
  (teardown! [_ testData]
    (cbclients/shutdown-pool testData)))

(defn set-client [dcpclient]
  (NewSetClient. nil nil nil dcpclient))

(defrecord CouchbaseCounterClient [cluster bucket collection]
  client/Client
  (open! [this testData node]
    (merge this (cbclients/get-client-from-pool testData)))

  (setup! [this test]
    (do (try
          (.remove ^Collection collection "jepsen")
          (catch CouchbaseException _))
        (try (.increment ^BinaryCollection (.binary ^Collection collection)
                         "jepsen"
                         (doto (clientUtils/get-increment-ops {:durability-level 1})
                           (.initial  (:init-counter-value test))
                           (.delta 0)))
             (catch CouchbaseException e
               (warn (.getMessage e))))))

  (invoke! [this testData op]
    (case (:f op)
      :add (do-counter-add collection op)
      :read (do-counter-read collection op)))

  (close! [_ _])
  (teardown! [this test]
    (cbclients/shutdown-pool test)))

(defn counter-client []
  (CouchbaseCounterClient. nil nil nil))
