(ns couchbase.collections-manifest
  (:require [clojure.set :as set]))

(defn manifest-map-entry->collections-manifest-entry
  "Transforms a manifest map entry into a collections manifest entry."
  [[scope-name collections]]
  {:name scope-name
   :collections (mapv #(hash-map :name %) collections)})

(defn manifest-map->collections-manifest
  "Transforms a manifest map to a collection manifest."
  [manifest-map]
  {:scopes (mapv manifest-map-entry->collections-manifest-entry
                 manifest-map)})

(defn manifest-map-entry->pair
  "Transforms a manifest entry [s1 [c1 c2 c3]] to [[s1 c1] [s1 c2] [s1 c3]]"
  [[scope collections]]
  (map #(vector scope %) collections))

(defn manifest-map-seq
  "Yields a sequence of pairs of scope and collection names."
  [manifest-map]
  (apply concat (mapv manifest-map-entry->pair manifest-map)))

(defn manifest-map-add
  "Adds collections to scope creating scope if it doesn't already exist."
  ([manifest-map scope & collections]
   (apply update manifest-map scope (fnil conj #{}) collections)))

(defn build-manifest-map
  "Returns a manifest-map with the specified number of scopes and collections
   which includes the default scope and collection."
  [no_of_scopes no_of_collections]
  (let [scpnames (map #(str "s" %) (range no_of_scopes))
        colnames (map #(str "c" %) (range no_of_collections))
        manifest (reduce #(apply manifest-map-add %1 %2 colnames) {} scpnames)]
    (update (set/rename-keys manifest {"s0" "_default"})
            "_default"
            #(replace {"c0" "_default"} %))))
