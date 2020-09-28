(ns couchbase.core
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase
             [util :as util]
             [cbjcli :as cbjcli]
             [workload :as workload]]
            [dom-top.core :as domTop]
            [jepsen
             [cli :as cli]
             [control :as c]
             [db :as db]
             [nemesis :as nemesis]
             [os :as os]
             [tests :as tests]]
            [jepsen.nemesis.time])
  (:gen-class))

(defn couchbase-remote
  "Initialisation logic for remote Couchbase nodes"
  []
  (let [collected-logs (atom [])]
    (reify
      db/DB
      (setup!    [_ testData node] (util/setup-node testData node))
      (teardown! [_ testData node] (util/teardown testData))

      db/Primary
      (setup-primary! [_ testData node]
        (util/setup-cluster testData node)
        (compare-and-set! (:db-intialized testData) false true))

      db/LogFiles
      (log-files [_ testData node]
        ;; Following the update from Jepsen 0.1.11 -> 0.1.14, this function
        ;; is for some reason being called multiple times for the same
        ;; node. I'm not sure what is triggering this, but it causes issues
        ;; for the cbcollects. Keep a list of nodes for which log collection
        ;; has been triggered, and return nil if we receive a duplicate
        ;; request.
        (if (->> (swap-vals! collected-logs conj node)
                 (first)
                 (not-any? #{node}))
          (util/get-remote-logs testData)
          (warn "Ignoring duplicate log collection request"))))))

(defn couchbase-cluster-run
  "Initialisation logic for cluster-run nodes"
  []
  (let [cluster-run-future (atom nil)
        collected-logs (atom {})]
    (reify
      db/DB
      (setup! [_ testData node] nil)
      (teardown! [_ testData node]
        (some-> @cluster-run-future future-cancel))

      db/Primary
      (setup-primary! [_ testData node]
        (reset! cluster-run-future (util/start-cluster-run testData))
        (util/setup-cluster testData node))

      db/LogFiles
      (log-files [_ testData node]
        ;; Avoid the same duplicate logging issue as above
        (when (->> (swap-vals! collected-logs assoc node :started)
                   (first)
                   (keys)
                   (not-any? #{node}))
          (util/get-cluster-run-logs testData node)
          ;; Since all node get killed during teardown, we need to hang on
          ;; log-files until all nodes have finished collecting logs.
          (swap! collected-logs assoc node :done)
          (while (->> @collected-logs (vals) (some #{:started}))
            (Thread/sleep 500)))))))

(defn validate-opts
  "Validate options. Individual options are validated during parsing, but once
  all options have been parsed we need to check the resulting map to ensure the
  combination of options is valid."
  [opts]
  (when (:cluster-run opts)
    (if-not (:package opts)
      (throw (RuntimeException. "--cluster-run requires --package parameter")))
    (if-not (:node-count opts)
      (throw (RuntimeException. "--cluster-run requires --node-count parameter")))
    (if (:manipulate-disks opts)
      (throw (RuntimeException. "--manipulate-disks cannot be used with --cluster-run"))))
  (if (and (not= (:durability opts) [100 0 0 0])
           (or (not= (:replicate-to opts) 0)
               (not= (:persist-to opts) 0)))
    (throw (RuntimeException.
            "Cannot combine sync-rep --durability with observe based --replicate-to or --persist-to")))
  (when (= (:workload opts) "disk-failure")
    (if-not (:manipulate-disks opts)
      (throw (RuntimeException. "disk-failover workload requires --manipulate-disks option")))
    (if (not= (:cycles opts 1) 1)
      (throw (RuntimeException. "disk-failover workload only supports a single \"cycle\""))))
  (when (and (or (= "set-kill" (:workload opts))
                 (= "kill" (:workload opts)))
             (= :suspend-process (:scenario opts)))
    (if (nil? (:process-to-suspend opts))
      (throw (RuntimeException. "For suspend-process scenario \"--process-to-suspend\" must be specified"))))
  (when (and (:doc-padding-size opts) (not (:use-json-docs opts)))
    (throw (RuntimeException. "To use --doc-padding-size \"--use-json-docs\" must be specified"))))

;; The actual testcase, merge the user options, basic parameters and workload
;; parameters into something that can be passed into Jepsen to run
(defn cbtest
  "Run the test"
  [opts]
  (validate-opts opts)
  ;; opts passed to this function come straight from cli parsing
  ;; these ops are then passed to workload
  (as-> opts opts
    ;; Construct base test case
    (merge tests/noop-test
           opts
           ;; generic parameters
           {:name "Couchbase"
            :db (if (opts :cluster-run)
                  (couchbase-cluster-run)
                  (couchbase-remote))
            :os os/noop
            :db-intialized (atom false)})
    ;; If cluster-run is specified, override the nodes and disable ssh
    (if (:cluster-run opts)
      (assoc opts
             :nodes (map #(str "127.0.0.1:" %)
                         (range 9000 (+ 9000 (:node-count opts))))
             :ssh (assoc (:ssh opts) :dummy? true))
      opts)
    ;; If package is a build dir, and not cluster-run, tar build to deploy
    (if (and (= :tar (:type (:package opts)))
             (not (:cluster-run opts)))
      (update-in opts [:package :package] util/tar-build)
      opts)
    ;; Construct the test case by merging workload parameters with options
    (merge opts ((workload/get-workload-opts (opts :workload)) opts))
    (merge opts ((workload/get-workload-fn (opts :workload)) opts))))

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
     (fn [nemesis testData op]
       (if (= (:workload-type testData) :legacy)
         (try
           (invoke-compat! nemesis testData op)
           (catch Exception e
             (if (:control-atom testData)
               (do
                 (compare-and-set! (:control-atom testData) :continue :abort)
                 (error "Caught exception in nemesis, aborting test.")
                 (throw e))
               (do
                 (error "Caught exception in nemesis and couldn't abort test, will hard exit.")
                 (System/exit 1)))))
         (invoke-compat! nemesis testData op)))))

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
       (domTop/with-retry [attempts 5]
         (apply download args)
         (catch ArrayIndexOutOfBoundsException _
           (warn "Encountered clj-ssh issue #59 during log download")
           (if (pos? attempts)
             (retry (dec attempts))
             (error "Log download failed due to clj-ssh issue #59")))
         (catch Exception e
           (if (pos? attempts)
             (retry (dec attempts))
             (error (str "Log download failed due to exception " (.getMessage e)))))))))

  ;; Now parse args and run the test
  (let [testData (cli/single-test-cmd {:test-fn  cbtest
                                       :opt-spec cbjcli/extra-cli-options})
        serve (cli/serve-cmd)]
    (cli/run! (merge testData serve) args)))
