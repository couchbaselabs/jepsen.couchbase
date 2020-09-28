(ns couchbase.workload
  (:require [clojure.string :refer [lower-case]]
            [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase
             [checker :as cbchecker]
             [cbclients :as cbclients]
             [clients :as clients]
             [seqchecker :as seqchecker]
             [util :as util]]
            [jepsen
             [checker :as checker]
             [independent :as independent]
             [nemesis :as nemesis]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [knossos.model :as model]))

;; Workload loading helpers

(defn- get-namespaced-workload
  "Get namespaced workload by name"
  [wl-name func]
  (try
    (require (symbol (format "couchbase.workload.%s" (lower-case wl-name))))
    (resolve (symbol (format "couchbase.workload.%s/workload-%s"
                             (lower-case wl-name)
                             (case func :opts "opts" :fn "fn"))))
    ;; Return nil if the workload does not exist
    (catch java.io.FileNotFoundException _)))

(defn get-workload-fn
  "Given a workload name return the corresponding workload creation function"
  [wl-name]
  (or (get-namespaced-workload wl-name :fn)
      (throw (RuntimeException. (format "Workload %s not found" wl-name)))))

(defn get-workload-opts
  "Given a workload name return the corresponding default options map"
  [wl-name]
  (or (get-namespaced-workload wl-name :opts)
      (throw (RuntimeException. (format "Workload %s not found" wl-name)))))

;; Generator Helpers

(defrecord SetWorkloadGenWrapper [gen max_add]
  gen/Generator
  (op [this test ctx]
    (when-let [[op gen'] (gen/op gen test ctx)]
      (case (:f op)
        :add [op (SetWorkloadGenWrapper. gen' (max max_add (:value op)))]
        :read [(assoc op :value max_add)
               (SetWorkloadGenWrapper. gen' max_add)]
        [op (SetWorkloadGenWrapper. gen' max_add)])))

  (update [this test ctx event] this))

(defn wrap-set-generator
  "Wrap a set workload generator to track the largest attempted add value, then
  automatically insert that value into any read requests. This is required to
  ensure the set client knows which keys to probe, as the op history is no
  longer accessbile to clients when using pure generators. Note that for
  performance reasons the wrapped generator does not propagate updates."
  [gen]
  (SetWorkloadGenWrapper. gen 0))

(defrecord Stopable [gen]
  gen/Generator
  (op [this test ctx]
    (when-let [[op gen'] (gen/op gen test ctx)]
      (if (= (:type op) :stop-test)
        (gen/op (gen/log "Stopping test") test ctx)
        [op (Stopable. gen')])))
  (update [this test ctx event]
    (if (and (= :nemesis (:process event))
             (contains? event :exception))
      (gen/log "Aborting test due to nemesis failure.")
      this)))

(defn wrap-generators
  "Wrap nemesis and client generators into a combined generator. Add handling
  code to allow aborting test. Note that updates are not propagated down for
  performance reasons."
  [client-gen nemesis-gen]
  (Stopable. (gen/any (gen/nemesis nemesis-gen client-gen)))) ;; TODO: Add timeout

;; Register Workload Helpers

(defn- register-op-gen
  "Returns the base op generator for register workloads"
  [opts]
  (gen/mix
   (cond (some #{:sequential} (:use-checker opts))
         [(gen/repeat {:f :read})
          (map (fn [x] {:f :write
                        :replicate-to (:replicate-to opts)
                        :persist-to (:persist-to opts)
                        :durability-level (util/random-durability-level
                                           (:durability opts))
                        :json (:use-jsoc-docs opts)
                        :value x})
               (range))]

         (:cas opts)
         [(gen/repeat {:f :read})
          #(gen/once {:f :write
                      :replicate-to (:replicate-to opts)
                      :persist-to (:persist-to opts)
                      :durability-level (util/random-durability-level
                                         (:durability opts))
                      :json (:use-json-docs opts)
                      :value (rand-int 5)})
          #(gen/once {:f :cas
                      :replicate-to (:replicate-to opts)
                      :persist-to (:persist-to opts)
                      :durability-level (util/random-durability-level
                                         (:durability opts))
                      :json (:use-json-docs opts)
                      :value [(rand-int 5) (rand-int 5)]})]

         :else
         [(gen/repeat {:f :read})
          #(gen/once {:f :write
                      :replicate-to (:replicate-to opts)
                      :persist-to (:persist-to opts)
                      :durability-level (util/random-durability-level
                                         (:durability opts))
                      :value (rand-int 50)})])))

(defn register-client-gen
  "Return a wrapped generator for register workloads"
  [opts]
  (independent/concurrent-generator
   (:doc-threads opts)
   (range)
   (fn [_]
     (cond->> (register-op-gen opts)
       (pos? (:rate opts 0)) (gen/stagger (/ (:rate opts)))))))

(defn get-register-checker
  "Return a checker by name"
  [checker-name]
  (case checker-name
    :linearizable (let [opts {:model (model/cas-register :nil)}]
                    {:linear (checker/linearizable opts)})
    :sequential {:sequential (seqchecker/sequential)}))

(defn register-common
  "Return a map of common parameters for register workloads"
  [opts]
  {:concurrency (* (:doc-threads opts) (:doc-count opts))
   :client (clients/register-client)
   :checker (checker/compose
             (merge
              {:indep (independent/checker
                       (checker/compose
                        (apply merge
                               {:timeline (timeline/html)}
                               (map get-register-checker
                                    (or (opts :use-checker) [:linearizable])))))
               :sanity (cbchecker/sanity-check)}
              (if (opts :perf-graphs)
                {:perf (checker/perf)})))})

;; Set Workload Helpers

(defn get-set-checker
  "Return a set checker by name"
  [checker-name]
  (case checker-name
    :basic {:set (checker/set)}
    :extended {:set (cbchecker/extended-set-checker)}))

(defn set-common
  "Return a map of common parameters for set workloads"
  [opts checker]
  {:client (if (:dcp-set-read opts)
             (clients/set-client (cbclients/dcp-client))
             (clients/set-client nil))
   :checker (checker/compose
             (merge
              {:timeline (timeline/html)
               :sanity (cbchecker/sanity-check)}
              (get-set-checker checker)
              (if (opts :perf-graphs)
                {:perf (checker/perf)})))})
