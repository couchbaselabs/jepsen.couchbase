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
    (setup!    [_ test node] (util/setup-node))
    (teardown! [_ test node] (util/teardown))

    db/Primary
    (setup-primary! [_ test node]
      (util/setup-cluster test node))

    db/LogFiles
    (log-files [_ test node]
      (when (test :get-cbcollect)
        (info "Generating cbcollect...")
        (c/su (c/exec (keyword "/opt/couchbase/bin/cbcollect_info")
                      "/opt/couchbase/var/lib/couchbase/logs/cbcollect.zip")))
      (c/su (c/exec :chmod :-R :a+rx "/opt/couchbase"))
      (->> (c/exec :ls "/opt/couchbase/var/lib/couchbase/logs")
           (str/split-lines)
           (map #(str "/opt/couchbase/var/lib/couchbase/logs/" %))))))


;; Jepsen has a predefine os module for centos, but it does a bunch of stuff that
;; we don't need, and has a tendency to break our vagrants; so we define our own
(def centos
  (reify os/OS
    (setup! [_ test node]
      (c/su (centos/install [:ntpdate])))
    (teardown! [_ test node])))


(defn cbtest
  "Run the test"
  [opts]
  (merge tests/noop-test
         opts
         {:name "Couchbase"
          :db (couchbase)
          :os centos
          :replicas 1
          :replicate-to 0}
         (as-> (opts :workload) %
               (workload/workloads %)
               (% opts))))

(defn parse-int [x] (Integer/parseInt x))

(def extra-cli-options
  [[nil "--workload WORKLOAD"
    "The workload to run"
    :default  nil
    :validate [#(workload/workloads %) (cli/one-of workload/workloads)]]
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
  ;; We modify the guest additions configuration to disable time syncing and
  ;; reload the module
  (alter-var-root
    (var jepsen.nemesis.time/install!)
    (fn [real_install!]
      (fn []
        (if (= (c/exec* "if [ -f /etc/rc.d/init.d/vboxadd-service ]; then echo true; fi")
               "true")
          (if (= 0 (count (c/exec :grep :-e "--disable-timesync"
                                  "/etc/rc.d/init.d/vboxadd-service" "||" :true)))
            (do
              (info "Detected virtualbox timesync, trying to disable...")
              (c/su (c/exec :sed :-i
                            "s/daemon $binary >/daemon $binary --disable-timesync >/"
                            "/etc/rc.d/init.d/vboxadd-service"))
              (if-not (= 0 (count (c/exec :grep :-e "--disable-timesync"
                                          "/etc/rc.d/init.d/vboxadd-service"
                                          "||" :true)))
                (do
                  (info "Modified vbox guest additions config to disable timesync")
                  (c/su (c/exec :service :vboxadd-service :restart))
                  (info "Restarted guest additions"))
                (do
                  (error "Failed to modify vbox additions config, can't disable timesync!")
                  (throw (RuntimeException. "Failed to disable virtualbox timesync")))))
            (info "Virtualbox timesync already disabled"))
          (info "Virtualbox guest additions dont seem to be installed"))
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
  (let [test  (cli/single-test-cmd {:test-fn  cbtest
                                    :opt-spec extra-cli-options})
        serve (cli/serve-cmd)]
    (-> (merge test serve)
        (cli/run! args))))
    
 
