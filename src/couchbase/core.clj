(ns couchbase.core
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [couchbase [nemesis  :as nemesis]
                       [util     :as util]
                       [workload :as workload]]
            [jepsen [cli     :as cli]
                    [control :as c]
                    [db      :as db]
                    [os      :as os]
                    [tests   :as tests]]
            [jepsen.os.centos :as centos]
            [slingshot.slingshot :refer [try+]]))

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
      (let [install-dir (or (:path (test :package))
                            "/opt/couchbase")]
        (when (test :get-cbcollect)
          (info "Generating cbcollect...")
          (c/su (c/exec (str install-dir "/bin/cbcollect_info")
                        (str install-dir "/var/lib/couchbase/logs/cbcollect.zip"))))
        (c/su (c/exec :chmod :-R :a+rx "/opt/couchbase"))
        (try
          (->> (c/exec :ls (str install-dir "/var/lib/couchbase/logs"))
               (str/split-lines)
               (map #(str install-dir "/var/lib/couchbase/logs/" %)))
          (catch RuntimeException e
            (warn "Error getting logfiles")
            []))))))

;; The only utility we actually need to install on our vagrants seems to be
;; ntpdate, so detect which pacakge manager to use and install it
(def os
  (reify os/OS
    (setup! [_ test node]
      (c/su (c/exec (util/get-package-manager) :install :-y :ntpdate)))
    (teardown! [_ test node])))


(defn cbtest
  "Run the test"
  [opts]
  (merge tests/noop-test
         opts
         {:name "Couchbase"
          :db (couchbase)
          :os os
          :replicas 1
          :replicate-to 0}
         (as-> (opts :workload) %
               (workload/workloads %)
               (% opts))))

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
   [nil "--get-cbcollect BOOL"
    "Generate a cbcollect at the end of the run?"
    :parse-fn #{"true"}
    :default false
    :validate [#{"true" "false"} "Must be true or false"]]
   [nil "--perf-graphs BOOL"
    "Output performance graphs? (Requires gnuplot)"
    :parse-fn #{"true"}
    :default false]])


;; Sequentially run multiple workloads, exiting on failure
(defn multi-test [single-test]
  {"multitest"
   (assoc single-test :run
          (fn [{:keys [options]}]
            (doseq [wl (str/split (options :workload) #",")]
              (info "***** Running workload:" wl "*****")
              ((single-test :run) {:options (assoc options :workload wl)}))))})

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

  ;; Jepsen 0.1.10 has a bug in the clock nemesis causing error handling to fail
  ;; when ntp isnt installed. This will be fixed Jepsen 0.1.11, but that isn't
  ;; released yet, so add an override to jepsen.control/exec that prevents
  ;; throwing the error
  (alter-var-root
   (var jepsen.control/exec)
   (fn [real_exec]
     (fn [& commands]
       (if (= commands [:service :ntpd :stop])
         (try
           (real_exec :service :ntpd :stop)
           (catch RuntimeException e
             (warn "Discarding ssh error to circumvent jepsen bug")))
         (apply real_exec commands)))))

  
  ;; Now parse args and run the test
  (let [test        (cli/single-test-cmd {:test-fn  cbtest
                                          :opt-spec extra-cli-options})
        multitest   (multi-test (test "test"))
        serve       (cli/serve-cmd)]
    (-> (merge test multitest serve)
        (cli/run! args))))
