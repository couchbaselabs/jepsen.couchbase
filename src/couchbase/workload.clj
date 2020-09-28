(ns couchbase.workload
  (:require [clojure.string :refer [lower-case]]
            [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase.workload.legacy :as legacy]))

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

(defn- get-legacy-workload
  "Get a legacy workload by name"
  [wl-name]
  (-> "couchbase.workload.legacy/%s-workload"
      (format wl-name)
      (symbol)
      (resolve)))

(defn get-workload-fn
  "Given a workload name return the corresponding workload creation function"
  [wl-name]
  (or (get-namespaced-workload wl-name :fn)
      (get-legacy-workload wl-name)
      (throw (RuntimeException. (format "Workload %s not found" wl-name)))))

(defn get-workload-opts
  "Given a workload name return the corresponding default options map"
  [wl-name]
  (if-let [workload-opts (get-namespaced-workload wl-name :opts)]
    workload-opts
    ;; If we can't find a namespace with the request workload, attempt to
    ;; resolve a legacy workload. If a legacy workload with the requested name
    ;; exists, return a dummy options function, since legacy workloads don't
    ;; have such a function
    (if (get-workload-fn wl-name)
      (constantly {:workload-type :legacy})
      (throw (RuntimeException. (format "Workload %s not found" wl-name))))))
