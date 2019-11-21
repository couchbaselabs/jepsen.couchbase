(ns couchbase.clients-utils
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [clojure.string :as string]
            [slingshot.slingshot :refer [try+]])
  (:import com.couchbase.client.core.msg.kv.DurabilityLevel
           com.couchbase.client.java.kv.PersistTo
           com.couchbase.client.java.kv.ReplicateTo
           com.couchbase.client.java.kv.CommonDurabilityOptions
           com.couchbase.client.java.json.JsonObject
           com.couchbase.client.java.kv.GetResult))

;; Helper functions to apply durability options

(defn apply-durability-options!
  "Helper function to apply durability level to perform sync-writes"
  [mutation-options op]
  (if-let [level (case (int (op :durability-level 0))
                   0 nil
                   1 DurabilityLevel/MAJORITY
                   2 DurabilityLevel/MAJORITY_AND_PERSIST_ON_MASTER
                   3 DurabilityLevel/PERSIST_TO_MAJORITY)]
    (.durability ^CommonDurabilityOptions mutation-options level)))

(defn apply-observe-options!
  "Helper function to apply the old observe based replicate-to/persist-to"
  [mutation-options op]
  (when (or (pos? (op :replicate-to 0))
            (pos? (op :persist-to 0)))
    (let [replicate-to (case (int (op :replicate-to 0))
                         0 ReplicateTo/NONE
                         1 ReplicateTo/ONE
                         2 ReplicateTo/TWO
                         3 ReplicateTo/THREE)
          persist-to   (case (int (op :persist-to 0))
                         0 PersistTo/NONE
                         1 PersistTo/ONE
                         2 PersistTo/TWO
                         3 PersistTo/THREE)]
      (.durability ^CommonDurabilityOptions mutation-options persist-to replicate-to))))

(defn gen-string
  "Function to generate a random string of char of length n"
  [n]
  (string/join (repeatedly n #(char (+ 65 (rand-int 62))))))

(def document-padding-string (atom nil))  ; 4MB for register, 64KB for set

(defn create-int-json-obj
  "Creates a JsonObject and stores a int under the key \"val\""
  ([val]
   (let [jsonDoc (.put (JsonObject/create) (str "val") val)]
     (when-not (nil? @document-padding-string)
       (.put ^JsonObject jsonDoc (str "padding")
             @document-padding-string))
     jsonDoc)))

(defn get-int-from-get-result
  "Function to get the int value from the val key stored in a TransactionGetResult"
  [getResult]
  (let [jsonObj (.contentAsObject ^GetResult getResult)]
    (.getInt ^JsonObject jsonObj (str "val"))))