(ns couchbase.core
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [couchbase [util     :as util]
                       [workload :as workload]]
            [jepsen [cli     :as cli]
                    [control :as c]
                    [db      :as db]
                    [os      :as os]
                    [tests   :as tests]])
  (:gen-class))

(defn couchbase
  "Initialise couchbase"
  []
  (reify
    db/DB
    (setup!    [_ test node] (util/setup-node test))
    (teardown! [_ test node] (util/teardown test))

    db/Primary
    (setup-primary! [_ test node]
      (util/setup-cluster test node))

    db/LogFiles
    (log-files [_ test node]
      (util/get-logs test))))

;; The only utility we actually need to install on our vagrants seems to be
;; ntpdate, so detect which package manager to use and install it
(def os
  (reify os/OS
    (setup! [_ test node]
      (case (util/get-package-manager)
        :yum (c/su (c/exec :yum :install :-y :ntpdate))

        ;; On ubuntu check if ntpdate is installed before attempting to install,
        ;; this prevents dpkg being locked (which for some reason it often is)
        ;; from preventing tests when ntpdate is already installed
        :apt (if-not (re-find #"installed" (c/exec :apt :list :-qq :ntpdate))
               (c/su (c/exec :apt :install :-y :ntpdate)))))
    (teardown! [_ test node])))

;; The actual testcase, merge the user options, basic parameters and workload
;; parameters into something that can be passed into Jepsen to run
(defn cbtest
  "Run the test"
  [opts]
  ;; opts passed to this function come straight from cli parsing
  ;; these ops are then passed to workload
  (merge tests/noop-test
         ;; generic cli parameters
         {:name "Couchbase"
          :db (couchbase)
          :os os
          :hashdump (opts :hashdump)
          :ssh-private-key (opts :ssh-private-key)
          :package (opts :package)
          :ssh (opts :ssh)
          :nodes (opts :nodes)
          :test-count (opts :test-count)
          :get-cbcollect (opts :get-cbcollect)
          :time-limit (opts :time-limit)
          :perf-graphs (opts :perf-graphs)
          }
         ;; workload specific parameter
         (try
           (as-> (opts :workload) %
                 (format "couchbase.workload/%s-workload" %)
                 (resolve (symbol %))
                 (% opts))
           (catch NullPointerException _
             (let [msg (format "Workload %s does not exist" (opts :workload))]
               (fatal msg)
               (throw (RuntimeException. msg)))))
         ))

(defn parse-int [x] (Integer/parseInt x))

(def extra-cli-options
  [[nil "--package URL-OR-FILENAME"
    "Install this couchbase package, use preinstalled version if not given"
    :default nil
    :parse-fn util/get-package]
   [nil "--workload WORKLOAD"
    "The workload to run"
    :default  nil]
   [nil "--oplimit LIMIT"
    "Limit the total number of operations"
    :default nil
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--get-cbcollect"
    "Generate a cbcollect at the end of the run?"
    :default false]
   [nil "--perf-graphs"
    "Output performance graphs? (Requires gnuplot)"
    :default false]
   [nil "--hashdump"
    "Output hashtable dump from all vbuckets"
    :default false]
   [nil "--replicas REPLICAS"
    "Number of replicas"
    :default nil
    :parse-fn parse-int ]
   [nil "--replicate-to REPLICATE-TO"
    "Replicate-to value"
    :default nil
    :parse-fn parse-int]
   [nil "--rate RATE"
    "Rate of operations"
    :default nil
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--autofailover AUTOFAILOVER"
    "Enable autofailover"
    :default nil
    :parse-fn read-string
    :validate [#(boolean? %) "Must be boolean"]]
   [nil "--autofailover-timeout AUTOFAILOVER-TIMEOUT"
    "Autofailover timeout if autofailover is enabled"
    :default nil
    :parse-fn parse-int
    :validate [#(> % 5) "Must be greater than 5 seconds"]]
   [nil "--autofailover-maxcount AUTOFAILOVER-MAXCOUNT"
    "Autofailover max count if autofailover is enabled"
    :default nil
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--doc-count DOC-COUNT"
    "Number of documents"
    :default nil
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--doc-threads DOC-THREADS"
    "Number of threads per document"
    :default nil
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]])

(defn -main
  "Run the test specified by the cli arguments"
  [& args]

  ;; The following are a bunch of hacks that lets us modify aspects of Jepsen's
  ;; behaviour while depending on the released jepsen jar

  ;; Jepsen's fressian writer crashes the entire process if it encounters something
  ;; it doesn't know how to log, preventing the results from being analysed. We
  ;; don't care about fressian output, so just disable it
  (intern 'jepsen.store 'write-fressian! (fn [& args] (info "Not writing fressian")))

  ;; When running vagrant on top of virtualbox, the guest additions by default
  ;; frequently auto-syncs the nodes clocks, breaking the time skew nemesies.
  ;; We disable the virtualbox guest additions to prevent this.
  (alter-var-root
    (var jepsen.nemesis.time/install!)
    (fn [real_install!]
      (fn []
        (c/su (c/exec :systemctl :stop :vboxadd-service "|:"))
        (c/su (c/exec :systemctl :stop :virtualbox-guest-utils "|:"))
        (real_install!))))

  ;; Now parse args and run the test
  (let [test        (cli/single-test-cmd {:test-fn  cbtest
                                          :opt-spec extra-cli-options})
        serve       (cli/serve-cmd)]
    (-> (merge test serve)
        (cli/run! args))))
