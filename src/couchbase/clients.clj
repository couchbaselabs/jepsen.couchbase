(ns couchbase.clients
  (:require [clojure.tools.logging :refer :all]
            [couchbase.cbclients :as cbclients]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [slingshot.slingshot :refer [try+]])
  (:import com.couchbase.client.java.document.JsonDocument
           com.couchbase.client.java.document.JsonLongDocument))

;; Jepsen client for the register workloads. Convert operations from Jepsen
;; into an operation that can be performed against a couchbase cluster, then
;; pass this op to the cbclient.
(defrecord RegisterClient [cbclient]
  client/Client
  (open! [this test node]
    (cbclients/maybe-setup cbclient test)
    this)

  (setup! [this test])

  (invoke! [_ test op]
    (let [[rawkey opval] (:value op)
          opkey          (format "testdoc%03d" rawkey)]
      (case (:f op)
        :read  (try+
                 (let [document (cbclients/invoke cbclient test :get opkey)
                       cas      (if document (.cas document) nil)
                       value    (if document (.content document) :nil)
                       kvpair   (independent/tuple rawkey value)]
                   (assoc op :type :ok :value kvpair :cas cas))
                 (catch java.lang.RuntimeException e
                   (assoc op :type :fail :error e)))

        :write (try+
                 (let [value         (long opval)
                       document      (JsonLongDocument/create opkey value)]
                   (->> (cbclients/invoke cbclient test :upsert document)
                        (.cas)
                        (assoc op :type :ok :newcas)))
                 (catch java.util.concurrent.TimeoutException _
                   (assoc op :type :info :error :Timeout))
                 (catch java.lang.RuntimeException e
                   (assoc op :type :info :error e)))

        :cas   (try+
                 (if-let [current-doc (cbclients/invoke cbclient test :get opkey)]
                   (let  [current-val (.content current-doc)
                          current-cas (.cas current-doc)
                          cas-from    (long (opval 0))
                          cas-to      (long (opval 1))]
                     (if (= current-val cas-from)
                       (->> (JsonLongDocument/create opkey cas-to current-cas)
                            (cbclients/invoke cbclient test :replace)
                            (.cas)
                            (assoc op :type :ok :oldcas current-cas :newcas))
                       (assoc op :type :fail :error :ValueNotCasFrom
                                 :oldcas current-cas)))
                   (assoc op :type :fail :error :DocumentDoesNotExist))
                 (catch com.couchbase.client.java.error.CASMismatchException _
                   (assoc op :type :fail :error :CASMismatch))
                 (catch com.couchbase.client.java.error.DurabilityException _
                   (assoc op :type :info :error :CASChanged))
                 (catch java.util.concurrent.TimeoutException _
                   (assoc op :type :info :error :Timeout))))))

  (teardown! [this test])

  (close! [_ test]
    (cbclients/maybe-close cbclient)))

;; Wrapper as records aren't externally visible
(defn register-client [cbclient]
  (RegisterClient. cbclient))

;; Jepsen client for the set workloads. We use a bucket to model as set. Add
;; operations mean inserting a key into the bucket (we don't care about it's
;; value, only it's existence). Read operations are performed by streaming
;; all the buckets mutations with dcp. Delete operations are only supported
;; in a limited set of situations. There is no support for delete
;; operations in Jepsen's native set checker; we have our own custom checker
;; in couchbase.checker that supports deletes to some extent, but this has
;; only been tested for replicating a single specific issue. We take three
;; clients since the batch-insert client can only perform adds, and we need a
;; seperate dcp client anyway for reads.
(defrecord SetClient [addclient delclient dcpclient]
  client/Client
  (open! [this test node]
    (cbclients/maybe-setup addclient test)
    (cbclients/maybe-setup delclient test)
    (cbclients/maybe-setup dcpclient test)
    this)

  (setup! [this test])

  (invoke! [_ test op]
    (case (:f op)
      :noop (assoc op :type :ok)
      :add  (let [name   (str (:value op))
                  doc    (JsonDocument/create name)]
              (as-> (cbclients/invoke addclient test :insert doc) %
                    (if (some? %)
                        (assoc op :type :ok)
                        (assoc op :type :info))))
      :del  (let [key (str (:value op))]
              (cbclients/invoke delclient test :remove key)
              (assoc op :type :ok))

      :read (->> (cbclients/invoke dcpclient test :get-all-keys nil)
                 (map #(Integer/parseInt %))
                 (assoc op :type :ok :value))))

  (teardown! [this test])

  (close! [_ test]
    (cbclients/maybe-close addclient)
    (cbclients/maybe-close delclient)
    (cbclients/maybe-close dcpclient)))

(defn set-client [addclient delclient dcpclient]
  (SetClient. addclient delclient dcpclient))
