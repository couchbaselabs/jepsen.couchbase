(ns couchbase.core
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [couchbase
             [util     :as util]
             [workload :as workload]]
            [dom-top.core :refer [with-retry]]
            [jepsen
             [cli     :as cli]
             [control :as c]
             [db      :as db]
             [os      :as os]
             [tests   :as tests]])
  (:gen-class))

(defn couchbase
  "Initialise couchbase"
  []
  (let [collected-logs (atom [])]
    (reify
      db/DB
      (setup!    [_ test node] (util/setup-node test))
      (teardown! [_ test node] (util/teardown test))

      db/Primary
      (setup-primary! [_ test node]
        (util/setup-cluster test node)
        (compare-and-set! (test :db-intialized) false true))

      db/LogFiles
      (log-files [_ test node]
        ;; Following the update from Jepsen 0.1.11 -> 0.1.14, this function
        ;; is for some reason being called multiple times for the same
        ;; node. I'm not sure what is triggering this, but it causes issues
        ;; for the cbcollects. Keep a list of nodes for which log collection
        ;; has been triggered, and return nil if we receive a duplicate
        ;; request.
        (if (->> (swap-vals! collected-logs conj node)
                 (first)
                 (not-any? #{node}))
          (util/get-logs test)
          (warn "Ignoring duplicate log collection request"))))))

;; The actual testcase, merge the user options, basic parameters and workload
;; parameters into something that can be passed into Jepsen to run
(defn cbtest
  "Run the test"
  [opts]
  ;; opts passed to this function come straight from cli parsing
  ;; these ops are then passed to workload
  (merge tests/noop-test
         opts
         ;; generic parameters
         {:name "Couchbase"
          :db (couchbase)
          :os os/noop
          :db-intialized (atom false)}
         ;; workload specific parameter
         (try
           (as-> (opts :workload) %
             (format "couchbase.workload/%s-workload" %)
             (resolve (symbol %))
             (% opts))
           (catch NullPointerException _
             (let [msg (format "Workload %s does not exist" (opts :workload))]
               (fatal msg)
               (throw (RuntimeException. msg)))))))

(defn parse-int [x] (Integer/parseInt x))

(def extra-cli-options
  [[nil "--package URL-OR-FILENAME"
    "Install this couchbase package, use preinstalled version if not given"
    :parse-fn util/get-package]
   [nil "--install-path PATH"
    "The path of the couchbase install on the nodes"
    :default-fn (fn [opts] (or (-> opts :package :path) "/opt/couchbase"))]
   [nil "--workload WORKLOAD"
    "The workload to run"]
   [nil "--oplimit LIMIT"
    "Limit the total number of operations"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--[no-]cbcollect"
    "Generate a cbcollect at the end of the run?"
    :default true]
   [nil "--perf-graphs"
    "Output performance graphs? (Requires gnuplot)"
    :default false]
   [nil "--hashdump"
    "Output hashtable dump from all vbuckets"
    :default false]
   [nil "--replicas REPLICAS"
    "Number of replicas"
    :parse-fn parse-int]
   [nil "--replicate-to REPLICATE-TO"
    "Replicate-to value"
    :parse-fn parse-int]
   [nil "--rate RATE"
    "Rate of operations. A rate of 0 disables rate limiting"
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "Must be a non-negative number"]]
   [nil "--[no-]autofailover"
    "Enable autofailover?"]
   [nil "--[no-]server-group-autofailover"
    "Enable server group autofailover"]
   [nil "--[no-]disk-autofailover"
    "Enable disk autofailover?"]
   [nil "--autofailover-timeout AUTOFAILOVER-TIMEOUT"
    "Autofailover timeout if autofailover is enabled"
    :parse-fn parse-int
    :validate [#(> % 5) "Must be greater than 5 seconds"]]
   [nil "--disk-autofailover-timeout DISK-AUTOFAILOVER-TIMEOUT"
    "Autofailover timeout if autofailover is enabled"
    :parse-fn parse-int
    :validate [#(> % 5) "Must be greater than 5 seconds"]]
   [nil "--autofailover-maxcount AUTOFAILOVER-MAXCOUNT"
    "Autofailover max count if autofailover is enabled"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--doc-count DOC-COUNT"
    "Number of documents"
    :default 30
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--doc-threads DOC-THREADS"
    "Number of threads per document"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--recovery-type RECOVERY-TYPE"
    :parse-fn #(cond (= % "delta") :delta (= % "full") :full :else :invalid)
    :validate [#(not= :invalid %) "Must be delta or full"]]
   [nil "--failover-type FAILOVER-TYPE"
    :parse-fn #(cond (= % "hard") :hard (= % "graceful") :graceful :else :invalid)
    :validate [#(not= :invalid %) "Must be hard or graceful"]]
   [nil "--disrupt-count COUNT"
    "Number of nodes to disrupt"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--skip-teardown"
    "Skip teardown of Couchbase server"
    :default false]
   [nil "--bucket-type TYPE"
    "Type of bucket to create (persistent or ephemeral)"
    :parse-fn {"persistent" :couchbase "ephemeral" :ephemeral}
    :validate [some? "Bucket type must be 'persistent' or 'ephemeral'"]
    :default :couchbase]
   [nil "--server-groups-enabled"
    "Turn on server groups"
    :default false]
   [nil "--target-server-groups"
    "Nemesis will target server groups"
    :default false]
   [nil "--cycles CYCLES"
    "Number of nemesis cycles to run"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--disrupt-time DISRUPTION-TIME"
    "Number of seconds for which the nemesis will act"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--kv-timeout KV-TIMEOUT"
    "Timeout for kv operations before aborting with an ambiguous response"
    :parse-fn #(->> % (Double/parseDouble) (* 1000) (java.time.Duration/ofMillis))
    :default (java.time.Duration/ofSeconds 10)]
   [nil "--node-count NODE-COUNT"
    "Number of nodes to use for this test"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--server-group-count SERVER-GROUP-COUNT"
    "Number of nodes to use for this test"
    :default 1
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--scenario SCENARIO"
    :parse-fn keyword]
   [nil "--durability L0:L1:L2:L3"
    "Probability distribution for the durability level"
    :default [100 0 0 0]
    :parse-fn #(map parse-int (str/split % #":"))
    :validate [#(and (= (reduce + %) 100)
                     (= (count %) 4))]]
   [nil "--custom-vbucket-count CUSTOM-VBUCKET-COUNT"
    :parse-fn parse-int
    :validate [#(<= 1 % 1024) "Vbucket count must be between 1 and 1024"]]
   [nil "--cas"
    "Enable CAS operations"
    :default false]
   [nil "--eviction-policy EVICTION-POLICY"
    "Eviction policy for the bucket"
    :parse-fn {"full" "fullEviction" "value" "valueOnly"}
    :default "fullEviction"]
   [nil "--manipulate-disks"
    "Turn on the ability to inject disk failures"
    :default false]
   [nil "--transactions"
    "Turn on the multi-document transactions"
    :default false]
   [nil "--mixed-txns"
    "Allow individual transactions to contain both reads and write"
    :default false]
   [nil "--enable-memcached-debug-log-level"
    "Set memcached log level to debug on all nodes"
    :default false]])

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

  ;; This is such a hack, but we want to exit with unknown status if our nemesis
  ;; crashes. We haven't found a linearizability error, so to exit with failure
  ;; would be incorrect, but if our nemesis isn't taking effect then we want
  ;; some warning that a pass is probably meaningless. We catch any exception
  ;; from the nemesis invocation, then set test's control-atom to abort before
  ;; rethrowing the exception to ensure it is logged. The sanity checker will
  ;; then detect that the control-atom is set to abort and return unknown status.
  (alter-var-root
   (var jepsen.nemesis/invoke-compat!)
   (fn [invoke-compat!]
     (fn [nemesis test op]
       (try
         (invoke-compat! nemesis test op)
         (catch Exception e
           (if (:control-atom test)
             (do
               (compare-and-set! (:control-atom test) :continue :abort)
               (error "Caught exception in nemesis, aborting test.")
               (throw e))
             (do
               (error "Caught exception in nemesis and couldn't abort test, will hard exit.")
               (System/exit 1))))))))

  ;; The default resolution of the perf graphs is tiny, so render something
  ;; bigger to show more detail
  (alter-var-root
   (var jepsen.checker.perf/preamble)
   (fn [preamble]
     (fn [output-path]
       (assoc-in (vec (preamble output-path)) [1 5 :xs] '(1800 800)))))

  ;; We've encountered crashed during log collection that appear to be caused
  ;; by https://github.com/hugoduncan/clj-ssh/issues/59. Until it is fixed,
  ;; inject a retry loop for that issue, and just move on if we keep failing.
  (alter-var-root
   (var jepsen.control/download)
   (fn [download]
     (fn [& args]
       (with-retry [attempts 5]
         (apply download args)
         (catch ArrayIndexOutOfBoundsException _
           (warn "Encountered clj-ssh issue #59 during log download")
           (if (pos? attempts)
             (retry (dec attempts))
             (error "Log download failed due to clj-ssh issue #59")))))))

  ;; Now parse args and run the test
  (let [test (cli/single-test-cmd {:test-fn  cbtest
                                   :opt-spec extra-cli-options})
        serve (cli/serve-cmd)]
    (cli/run! (merge test serve) args)))
