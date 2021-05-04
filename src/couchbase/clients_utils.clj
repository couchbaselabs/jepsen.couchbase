(ns couchbase.clients-utils
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [clojure.string :as string]
            [slingshot.slingshot :refer [try+]]
            [couchbase.util :as util]
            [couchbase.cbjcli :as cbjcli])
  (:import (com.couchbase.client.java.kv LookupInSpecStandard
                                         Upsert
                                         InsertOptions
                                         CommonDurabilityOptions
                                         ReplicateTo
                                         PersistTo
                                         UpsertOptions
                                         GetResult
                                         LookupInResult
                                         MutateInOptions
                                         MutateInSpec
                                         LookupInSpec
                                         RemoveOptions
                                         ReplaceOptions
                                         IncrementOptions
                                         DecrementOptions)
           (java.util Collections
                      Arrays)
           (com.couchbase.client.core.msg.kv DurabilityLevel)
           (com.couchbase.client.java.json JsonObject)
           (com.couchbase.client.java Collection)
           (com.couchbase.client.core.error DocumentNotFoundException))
  (:gen-class))

;; Helper functions to apply durability options

(defn apply-durability-options!
  "Helper function to apply durability level to perform sync-writes"
  [mutation-options op]
  (if-let [level (case (int (op :durability-level 0))
                   0 DurabilityLevel/NONE
                   1 DurabilityLevel/MAJORITY
                   2 DurabilityLevel/MAJORITY_AND_PERSIST_TO_ACTIVE
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

; Mutation Option builders
(defn options-builder
  [obj levels]
  (doto obj
    (apply-durability-options! levels)
    (apply-observe-options! levels)))

(defn get-insert-ops
  "Function get InsertOptions obj given a hash map of durability levels
  {:durability-level, :replicate-to, :persist-to}"
  [levels]
  (options-builder (InsertOptions/insertOptions) levels))

(defn get-upsert-ops
  "Function get UpsertOptions obj given a hash map of durability levels
  {:durability-level, :replicate-to, :persist-to}"
  [levels]
  (options-builder (UpsertOptions/upsertOptions) levels))

(defn get-replace-ops
  "Function get ReplaceOptions obj given a hash map of durability levels
  {:durability-level, :replicate-to, :persist-to}"
  [levels cas]
  (.cas (options-builder (ReplaceOptions/replaceOptions) levels) cas))

(defn get-remove-ops
  "Function get RemoveOptions obj given a hash map of durability levels
  {:durability-level, :replicate-to, :persist-to}"
  [levels]
  (options-builder (RemoveOptions/removeOptions) levels))

(defn get-mutate-in-ops
  "Function get MutateInOptions obj given a hash map of durability levels
  {:durability-level, :replicate-to, :persist-to}"
  [levels]
  (options-builder (MutateInOptions/mutateInOptions) levels))

(defn get-increment-ops
  "Function get IncrementOptions obj given a hash map of durability levels
  {:durability-level, :replicate-to, :persist-to}"
  [levels]
  (options-builder (IncrementOptions/incrementOptions) levels))

(defn get-decrement-ops
  "Function get DecrementOptions obj given a hash map of durability levels
  {:durability-level, :replicate-to, :persist-to}"
  [levels]
  (options-builder (DecrementOptions/decrementOptions) levels))

(def document-padding-string (atom nil))  ; 4MB for register, 64KB for set

(defn gen-string
  "Function to generate a random string of char of length n"
  [n]
  (string/join (repeatedly n #(char (+ 65 (rand-int 62))))))

(defn parse-padding-and-set
  "Function to take the document padding arg and generate a padding string"
  [padding-size]
  (reset! document-padding-string (gen-string (* padding-size 512))))

(defn create-int-json-obj
  "Creates a JsonObject and stores a int under the key \"val\""
  ([val]
   (let [jsonDoc (.put (JsonObject/create) (str "val") val)]
     (when-not (nil? cbjcli/*padding-size*)
       (when-not @document-padding-string
         (parse-padding-and-set cbjcli/*padding-size*))
       (.put ^JsonObject jsonDoc (str "padding")
             @document-padding-string))
     jsonDoc)))

(defn get-int-from-get-result
  "Function to get the int value from the val key stored in a GetResult"
  [getResult]
  (let [jsonObj (.contentAsObject ^GetResult getResult)]
    (.getInt ^JsonObject jsonObj (str "val"))))

(defn get-int-from-look-up-obj
  "Function to get the first value out of a LookupInResult obj as an int"
  [obj]
  (.contentAs ^LookupInResult obj 0 Integer))

(defn perform-subdoc-upsert
  "Function to perform an upsert on a the key \"val\" in a Json document"
  [collection key val levels]
  (try (.mutateIn ^Collection collection
                  ^String key
                  ^Arrays (Collections/singletonList ^Upsert (MutateInSpec/upsert "val" val))
                  ^MutateInOptions (get-mutate-in-ops levels))
       (catch DocumentNotFoundException _
         (.insert ^Collection collection
                  ^String key
                  ^JsonObject (create-int-json-obj val)
                  (get-insert-ops levels)))))

(defn perform-subdoc-get
  "Function to get value at the key \"val\" in a Json document using a sub-doc get"
  [collection key]
  (.lookupIn ^Collection collection
             ^String key
             ^Arrays (Collections/singletonList ^LookupInSpecStandard (LookupInSpec/get "val"))))
