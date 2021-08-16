(ns couchbase.util
  (:require [clojure.java.shell :as shell]
            [clojure.set    :as set]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn error fatal]]
            [clojure.string :as str]
            [dom-top.core :as domTop]
            [clj-http.client :as client]
            [cheshire.core :as json]
            [couchbase.collections-manifest :as manifest]
            [jepsen
             [control :as c]
             [net :as net]
             [store :as store]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import java.io.File
           clojure.lang.ExceptionInfo))

(defn get-node-name
  "Get the ns_server otpNode name for a given node"
  [node]
  ;; Handle special case for cluster-run nodes
  (if-let [[_ port] (re-matches #"127.0.0.1:(\d+)" node)]
    (format "n_%d@127.0.0.1" (-> port (Integer/parseInt) (- 9000)))
    (str "ns_1@" node)))

(defn get-node-id
  "Get the Jepsen node ID from the ns_server otpNode name"
  [name]
  (let [[_ cri ni node] (re-matches #"n(s?)_(\d+)@([\d\.]+)" name)
        cluster-run (empty? cri)
        node-index (Integer/parseInt ni)]
    (if cluster-run
      (str "127.0.0.1:" (+ 9000 node-index))
      node)))

(defn get-connection-string
  "Get the connection string to pass to the SDK"
  [node]
  (if-let [[_ port] (re-matches #"127.0.0.1:(\d+)" node)]
    ;; For cluster-run nodes we need to pass the custom memcached port
    (str "127.0.0.1:" (-> port (Integer/parseInt) (- 9000) (* 2) (+ 12000)))
    node))

(defn is-cluster-run-node
  [node-string]
  (re-matches #"127.0.0.1:(\d+)" node-string))

(defn get-node-id-from-node-hostname
  [node-string]
  (-> (get-node-name node-string) (str/split #"@") (first)))

(defn perform-kill-command
  [node process kill-arg]
  (if (is-cluster-run-node node)
    (let [node-id (get-node-id-from-node-hostname node)]
      (case process
        ;; On some systems (notably OSX) pgrep seems to have an issue where with very
        ;; long command argument lists it doesn't search the list, even with the -f
        ;; option, so pipe the output through normal grep
        :babysitter (shell/sh "kill" (str kill-arg) (str "$(pgrep -lf memcached | grep -E \""
                                                         node-id
                                                         "[^0-9]\" cut -d \" \" -f 1 | head -n 1)"))

        :ns-server (shell/sh "kill" (str kill-arg) (str "$(pgrep -lf memcached | grep -E \""
                                                        node-id
                                                        "[^0-9]\" cut -d \" \" -f 1 | tail -n +2)"))

        :memcached (shell/sh "kill" (str kill-arg) (str "$(pgrep -lf memcached | grep -E \""
                                                        node-id
                                                        "[^0-9]\" cut -d \" \" -f 1)"))))
    (case process
      :babysitter (c/on node (c/su (c/exec :bash :-c (str "kill " (str kill-arg) " $(pgrep beam.smp | head -n 1)"))))
      :ns-server (c/on node (c/su (c/exec :bash :-c (str "kill " (str kill-arg) " $(pgrep beam.smp | tail -n +2)"))))
      :memcached (c/on node (c/su (c/exec :bash :-c (str "kill " (str kill-arg) " $(pgrep memcached)")))))))

(defn kill-process
  "Kill a Couchbase Server process of the target node"
  [node process]
  (perform-kill-command node process "-SIGKILL"))

(defn halt-process
  "Function to halt a process"
  [node process]
  (perform-kill-command node process "-SIGSTOP"))

(defn continue-process
  "Function to continue a halted process"
  [node process]
  (perform-kill-command node process "-SIGCONT"))

(defn rest-call
  "Perform a rest api call"
  ([method endpoint] (rest-call method endpoint {}))
  ([method endpoint
    {:keys [target params options]
     :or   {target c/*host*}
     :as    kwargs}]
   (let [uri (if (re-matches #".*:[0-9]+" target)
               (str "http://" target endpoint)
               (str "http://" target ":8091" endpoint))
         common {:basic-auth ["Administrator" "abc123"]
                 :throw-entire-message true}
         options (merge common options)
         options (conj options (when params [:form-params params]))]
     (try
       (:body
        (case method
          :get  (client/get uri options)
          :put  (client/put uri options)
          :post (client/post uri options)))
       ;; Catch any exception and rethrow any exception so that we can log
       ;; the call which caused the exception.
       (catch ExceptionInfo e
         (warn "Rest call to" uri "with params" params "threw exception.")
         (if (= (int 503) (int (:status (ex-data e))))      ; See if there was a http 503 error
           (do (Thread/sleep 5000)                          ; Sleep for 5 sec then re-try
               (rest-call method endpoint kwargs))
           (throw e)))))))

;; On recent version versions of Couchbase Server /diag/eval is only accessible
;; from localhost, so we need to ssh into the node and curl from there. In
;; cluster-run scenarios we don't have any ssh sessions, in this case we call
;; curl from a local shell to keep argument handling identical.
(defn diag-eval
  ([params] (diag-eval c/*host* params))
  ([node params]
   (if (str/starts-with? node "127.0.0.1:")
     (shell/sh "curl" "-s" "-S" "--fail" "-u" "Administrator:abc123"
               (str "http://" node "/diag/eval")
               "-d" params)
     (c/on node (c/exec "curl" "-s" "-S" "--fail" "-u" "Administrator:abc123"
                        "http://localhost:8091/diag/eval"
                        "-d" params)))))

(defn initialise
  "Initialise a new cluster"
  [test]
  (let [base-path (:install-path test)
        data-path (str base-path "/var/lib/couchbase/data")
        index-path (str base-path "/var/lib/couchbase/data")]
    (rest-call :post "/nodes/self/controller/settings"
               {:params {:data_path data-path
                         :index_path index-path}})
    (rest-call :post "/node/controller/setupServices"
               {:params {:services "kv"}})
    (rest-call :post "/settings/web"
               {:params {:username "Administrator"
                         :password "abc123"
                         :port "SAME"}})
    (rest-call :post "/pools/default"
               {:params {:memoryQuota "256"}})))

(defmacro retry-with-exp-backoff
  "Evals body retrying after an exponential backoff period if an
   exception was raised. The body must be idempotent on failure."
  [dt rate retries & body]
  `(domTop/with-retry [dt#      ~dt
                       retries# ~retries]
     (try
       ~@body
       (catch Throwable e#
         (when (zero? retries#)
           (throw e#))
         (Thread/sleep dt#)
         (~'retry (* dt# ~rate) (dec retries#))))))

(defn add-node
  "Add node to the cluster"
  [node]
  (info "Adding node " node "to cluster")
  (retry-with-exp-backoff
   3000 1.3 5
   (rest-call :post "/controller/addNode"
              {:params {:hostname (str "http://" node)
                        :user "Administrator"
                        :password "abc123"
                        :services "kv"}})))

(defn add-nodes
  "Add nodes to the cluster"
  [nodes-to-add]
  (doseq [node nodes-to-add]
    (add-node node)))

(defn wait-for
  ([call-function desired-state] (wait-for call-function desired-state 60))
  ([call-function desired-state retries]
   (loop [state (call-function)
          attempts 0]
     (if (>= attempts retries)
       (throw (RuntimeException. (str "Desired state not achieved in " (str retries) " retries"))))
     (when (not= state desired-state)
       (info "waiting for " (str desired-state) " but have " (str state))
       (Thread/sleep 1000)
       (recur (call-function) (inc attempts))))))

(defn get-rebalance-status
  [target]
  (let [rebalance-info (rest-call :get "/pools/default/rebalanceProgress" {:target target})
        rebalance-info-map (json/parse-string rebalance-info true)]
    rebalance-info-map))

(defn wait-for-rebalance-complete
  ([rest-target] (wait-for-rebalance-complete rest-target 600))
  ([rest-target max-stuck]
   (loop [status-map {}
          retry-count 0
          stuck-count 0]
     (info "Rebalance status:" status-map)
     (info "Rebalance check count:" retry-count)
     (info "Rebalance stuck count:" stuck-count)
     (when (:errorMessage status-map)
       (info "Rebalance failed")
       (throw (RuntimeException. "Rebalance failed")))
     ; check if rebalance stuck
     (when (>= stuck-count max-stuck)
       (info "Rebalance stuck")
       (throw (RuntimeException. "Rebalance stuck")))
     (when (not= (:status status-map) "none")
       (Thread/sleep 1000)
       (let [new-status-map (get-rebalance-status rest-target)]
         (recur new-status-map
                (inc retry-count)
                (if (= status-map new-status-map) (inc stuck-count) stuck-count)))))))

(defn rebalance
  "Initiate a rebalance with the given parameters"
  ([known-nodes] (rebalance known-nodes nil))
  ([known-nodes eject-nodes]
   (let [known-nodes-str (->> known-nodes
                              (map get-node-name)
                              (str/join ","))
         eject-nodes-str (->> eject-nodes
                              (map get-node-name)
                              (str/join ","))
         valid-rest-targets (apply disj (set known-nodes) (set eject-nodes))
         rest-target (if (some? c/*host*)
                       c/*host*
                       (first valid-rest-targets))]
     (if-not rest-target
       (throw (ex-info "No control host is defined for rebalance" {:ejectedNodes eject-nodes-str
                                                                   :knownNodes known-nodes-str})))
     (if-not (contains? valid-rest-targets rest-target)
       (throw (ex-info "Current control host is not a valid rest target for rebalance" {:ejectedNodes eject-nodes-str
                                                                                        :knownNodes known-nodes-str})))
     (if eject-nodes
       (info "Rebalancing nodes" eject-nodes "out of cluster"))
     (rest-call :post "/controller/rebalance"
                {:target rest-target
                 :params {:ejectedNodes eject-nodes-str
                          :knownNodes known-nodes-str}})
     (wait-for-rebalance-complete rest-target)
     (info "Rebalance complete"))))

(defn failover
  "Failover a node using the specified fail-type."
  [fail-type call-node target]
  (let [endpoint (case fail-type
                   :hard "/controller/failOver"
                   :graceful "/controller/startGracefulFailover")]
    (retry-with-exp-backoff
     3000 1.3 5
     (rest-call :post endpoint {:target call-node
                                :params {:otpNode (get-node-name target)}}))))

(defn create-bucket
  "Create the default bucket"
  [bucket-type replicas eviction]
  (rest-call :post "/pools/default/buckets"
             {:params {:flushEnabled 1
                       :replicaNumber replicas
                       :evictionPolicy (name eviction)
                       :ramQuotaMB 100
                       :bucketType (name bucket-type)
                       :name "default"
                       :authType "sasl"
                       :saslPassword ""}}))

(defn set-vbucket-count
  "Set the number of vbuckets for new buckets"
  [testData]
  (if-let [num-vbucket (:custom-vbucket-count testData)]
    (diag-eval (format "ns_config:set(couchbase_num_vbuckets_default, %s)."
                       num-vbucket))))

(defn set-autofailover
  "Apply autofailover settings to cluster"
  [testData]
  (let [enabled (boolean (:autofailover testData))
        disk-enabled (boolean (:disk-autofailover testData))
        timeout (or (:autofailover-timeout testData) 6)
        disk-timeout (or (:disk-autofailover-timeout testData) 6)
        maxcount (or (:autofailover-maxcount testData) 3)]
    (rest-call :post "/settings/autoFailover"
               {:params {:enabled enabled
                         :timeout timeout
                         :maxCount maxcount
                         "failoverOnDataDiskIssues[enabled]" disk-enabled
                         "failoverOnDataDiskIssues[timePeriod]" disk-timeout}})))

(defn set-autoreprovision
  "Apply ephemeral autoreprovision settings to cluster"
  [testData]
  (rest-call :post "/settings/autoReprovision"
             {:params {:enabled (:autoreprovision testData)
                       :maxNodes (:autoreprovision-maxnodes testData)}}))

(defn wait-for-warmup
  "Wait for warmup to complete"
  []
  (let [retry-count (atom 0)]
    (while (re-find #"\"status\":\"warmup\"" (rest-call :get "/pools/default"))
      (if (> @retry-count 60)
        (throw (Exception. "bucket failed to warmup")))
      (swap! retry-count inc)
      (Thread/sleep 1000))))

(defn set-custom-cursor-drop-marks
  "Set the cursor dropping marks to a new value on all nodes"
  [testData]
  (let [lower_mark (nth (:custom-cursor-drop-marks testData) 0)
        upper_mark (nth (:custom-cursor-drop-marks testData) 1)
        config (format "cursor_dropping_lower_mark=%d;cursor_dropping_upper_mark=%d"
                       lower_mark
                       upper_mark)
        props (format "[{extra_config_string, \"%s\"}]" config)
        params (format "ns_bucket:update_bucket_props(\"default\", %s)." props)]
    (doseq [node (:nodes testData)]
      (diag-eval node params)))
  (c/with-test-nodes testData (kill-process c/*host* :memcached))
  ;; Before polling to check if we have warmed up again, we need to wait a while
  ;; for ns_server to detect memcached was killed
  (Thread/sleep 3000)
  (info "Waiting for memcached to restart")
  (wait-for-warmup))

(defn set-debug-log-level
  "Set log level of memcached to debug (1)"
  []
  (info "Setting memcached log level to debug")
  (rest-call :post "/pools/default/settings/memcached/global"
             {:params {:verbosity 1}}))

(defn disable-auto-compaction
  "Disables auto-compaction"
  []
  (info "Disabling auto-compaction")
  (rest-call :post "/controller/setAutoCompaction"
             {:params {"databaseFramgentationThreshold[size]" "undefined"
                       "parallelDBAndViewCompaction" false}}))

(defn upload-collections-manifest
  "Uploads the collections manifest."
  [collections-manifest]
  (let [response (rest-call :put "/pools/default/buckets/default/scopes"
                            {:params collections-manifest
                             :options {:content-type :json}})
        jsonresp (json/parse-string response true)
        endpoint (str "/pools/default/buckets/default/scopes/@ensureManifest/" (:uid jsonresp))]
    (retry-with-exp-backoff 3000 1.2 5
                            (rest-call :post endpoint))))

(defn testData->collection-aware?
  "Returns true if this is a collection aware test."
  [testData]
  (and (:collections testData) (:scopes testData)))

(defn testdata->manifest-map
  "Creates a manifest given test data."
  [testData]
  (manifest/build-manifest-map (:scopes testData) (:collections testData)))

(def testdata->seq-of-collections
  "Yields a seq of pairs of scope and collection names given testData
   caching the result to avoid recomputing it."
  (memoize
   (fn
     [testData]
     (manifest/manifest-map-seq (testdata->manifest-map testData)))))

(defn create-collections
  "Creates collections given test data."
  [testData]
  (-> (testdata->manifest-map testData)
      (manifest/manifest-map->collections-manifest)
      (upload-collections-manifest)))

(defn setup-cluster
  "Setup couchbase cluster"
  [testData node]
  (info "Creating couchbase cluster from" node)
  (let [nodes (:nodes testData)
        other-nodes (remove #(= node %) nodes)
        bucket-type (:bucket-type testData)
        num-replicas (:replicas testData)
        eviction-policy (if (= bucket-type :couchbase)
                          (:eviction-policy testData)
                          :noEviction)]
    (initialise testData)
    (add-nodes other-nodes)
    (set-vbucket-count testData)
    (if (> (count nodes) 1)
      (rebalance nodes))
    (set-autofailover testData)
    (set-autoreprovision testData)
    (create-bucket bucket-type num-replicas eviction-policy)
    (info "Waiting for bucket warmup to complete...")
    (wait-for-warmup)
    (when (testData->collection-aware? testData)
      (create-collections testData))
    (if (:custom-cursor-drop-marks testData)
      (set-custom-cursor-drop-marks testData))
    (if (:enable-memcached-debug-log-level testData)
      (set-debug-log-level))
    (if (:disable-auto-compaction testData)
      (disable-auto-compaction))
    (info "Setup complete")))

(defmacro swallow-exceptions
  "On success returns the evaled body, otherwise returns nil."
  [& body]
  `(try ~@body
        (catch Exception e#)))

(defn upload-location
  "Returns the upload location for a given package type."
  [pkg]
  (str "/tmp/couchbase." (name (:type pkg))))

(defn install
  "Installs a package."
  [pkg]
  (case (:type pkg)
    :deb (c/su (c/exec :apt :install :-y :--allow-downgrades (upload-location pkg)))
    :rpm (c/su (c/exec :yum :install :-y (upload-location pkg)))))

(defn uninstall
  "Removes an existing installation."
  [pkg]
  (case (:type pkg)
    :deb (c/su (c/exec :apt :remove :-y :--purge :couchbase-server))
    :rpm (c/su (c/exec :yum :remove :-y :couchbase-server))))

(defn reinstall
  "Reinstalls a package. Does not error if the uninstall fails."
  [pkg]
  (swallow-exceptions (uninstall pkg))
  (install pkg))

(defn install-tar
  "Handles the installation of tarball archives."
  [pkg]
  (c/su (c/exec :tar :-Pxf (upload-location pkg))))

(defn install-pkg
  "Handles the installation of deb or rpm packages."
  [pkg]
  (when-not (swallow-exceptions (install pkg))
    (reinstall pkg)))

(defn install-package
  "Installs a package."
  [pkg]
  (c/su (c/upload (:package pkg) (upload-location pkg)))
  (if (= (:type pkg) :tar)
    (install-tar pkg)
    (install-pkg pkg))
  (c/su (c/exec :rm (upload-location pkg))))

(defn wait-for-daemon
  "Wait until couchbase server daemon has started"
  []
  (domTop/with-retry [retry-count 30]
    (rest-call :get "/pools/")
    (catch Exception _
      (Thread/sleep 2000)
      (if (pos? retry-count)
        (retry (dec retry-count))
        (throw (Exception. "Cluster not reachable after 60s. Install broken?"))))))

(defn setup-devmapper-device
  "Mount a devmapper device as the Couchbase data directory"
  [test]
  (let [data-path (str (:install-path test) "/var/lib/couchbase/data")]
    (c/su (c/exec :dd "if=/dev/zero" "of=/tmp/cbdata.img" "bs=1M" "count=512")
          (c/exec :losetup "/dev/loop0" "/tmp/cbdata.img")
          (c/exec :dmsetup :create :cbdata :--table (c/lit "'0 1048576 linear /dev/loop0 0'"))
          (c/exec :mkfs.ext4 "/dev/mapper/cbdata")
          (c/exec :mkdir :-p data-path)
          (c/exec :mount :-o "noatime" "/dev/mapper/cbdata" data-path))))

(defn setup-tcp-packet-capture
  "Function to enable tcp packet capture on eth1"
  [testData node]
  (let [packet-capture-dir (str "/packet-capture")
        pack-dump-file (str (:name testData) "-" node ".pcap")
        tcp-dump-interface (:net-interface testData)]
    (c/su (c/exec :mkdir :-p packet-capture-dir))
    (info (str "packet dump file name " pack-dump-file))
    (c/su (c/exec :rm :-f "/var/run/daemonlogger.pid"))
    (c/ssh* {:cmd  (str "nohup sudo daemonlogger -d -l \"" packet-capture-dir "\" -n \"" pack-dump-file "\" -i " tcp-dump-interface " -S 262144 tcp > nohup-daemonlogger.log ")})))

(defn setup-node
  "Start Couchbase Server on a node"
  [testData node]
  (info "Setting up Couchbase Server")
  (let [package (:package testData)
        install-path (:install-path testData)
        server-path (str install-path "/bin/couchbase-server")
        config-path (str install-path "/var/lib/couchbase")
        data-path (str config-path "/data")]
    (when package
      (info "Installing package")
      (try
        (install-package package)
        (catch Exception e
          (fatal "Package installation failed with exception: " e)
          (throw e)))
      (info "Package installed"))
    (info "Setting up data paths")
    (if (:manipulate-disks testData)
      (setup-devmapper-device testData)
      (c/su (c/exec :mkdir :-p data-path)))
    (info "Changing permissions for" install-path)
    (c/su (c/exec :chmod :-R "a+rwx" install-path))
    (info "Starting daemon")
    (try
      (c/su (c/exec :systemctl :set-environment "CB_MAXIMIZE_LOGGER_CYCLE_SIZE=1"))
      (c/su (c/exec :systemctl :restart :couchbase-server))
      (catch RuntimeException e
        (c/ssh* {:cmd (str "CB_MAXIMIZE_LOGGER_CYCLE_SIZE=1 nohup " server-path " -- -noinput >> /dev/null 2>&1 &")})))
    (wait-for-daemon)
    (info "Daemon started")
    (if (and (and (:enable-tcp-capture testData) (:net-interface testData)) (not (:cluster-run testData)))
      (setup-tcp-packet-capture testData node)
      (info "TCP packet capture is not enabled"))))

(defn teardown
  "Stop the couchbase server instances and delete the data files"
  [testData]
  (try
    (if (and (:skip-teardown testData) (deref (:db-intialized testData)))
      (info "Skipping teardown of couchbase node")
      (let [path (:install-path testData)]
        (info "Tearing down couchbase node")
        (try
          (c/su (c/exec :systemctl :stop :couchbase-server))
          (catch RuntimeException e))
        (try
          (c/su (c/exec :killall :-9 :beam.smp))
          (catch RuntimeException e))
        (try
          (c/su (c/exec :killall :-9 :memcached))
          (catch RuntimeException e))
        (try
          (c/su (c/exec :umount :-l "/dev/mapper/cbdata"))
          (catch RuntimeException e))
        (try
          (c/su (c/exec :dmsetup :remove :-f "/dev/mapper/cbdata"))
          (catch RuntimeException e))
        (try
          (c/su (c/exec :losetup :-d "/dev/loop0"))
          (catch RuntimeException e))
        (try
          (c/su (c/exec :rm "/tmp/cbdata.img"))
          (catch RuntimeException e))
        (try
          (c/su (c/exec :rm :-rf (str path "/var/lib/couchbase")))
          (catch RuntimeException e (info "rm -rf " (str path "/var/lib/couchbase") " failed: " (str e))))
        (try
          (c/su (c/exec* "pkill -9 daemonlogger"))
          (catch Exception e))
        ;; Remove any leftover iptables rules from Jepsen's network partitions
        (locking teardown
          (domTop/with-retry [retry-count 5]
            (net/heal! (:net testData) testData)
            (catch RuntimeException e
              (warn "Failed to heal network," retry-count "retries remaining")
              (if (pos? retry-count)
                (retry (dec retry-count))
                (throw (RuntimeException. "Failed to heal network" e))))))))
    (info "Teardown Complete")
    (catch Exception e
      (throw (Exception. (str "teardown failed: " (str e)))))))

(defn get-version
  "Get the couchbase version running on the cluster"
  [node]
  (->> (rest-call :get "/pools/" {:target node})
       (re-find #"(?<=\"implementationVersion\":\")([0-9])\.([0-9])\.([0-9])")
       (rest)
       (map #(Integer/parseInt %))))

;; When pointed at a custom build, we need to place the install on each vagrant
;; node at the same path as it was built, or absolute paths in the couchbase
;; install will be broken. We tar the build with absolute paths to ensure we
;; put everything in the correct place
(defn tar-build
  [build]
  (let [package-file (File/createTempFile "couchbase" ".tar")]
    (info "TARing build...")
    (shell/sh "tar"
              "-Pcf"
              (.getCanonicalPath ^File package-file)
              "--owner=root"
              "--group=root"
              (.getCanonicalPath ^File build))
    (info "TAR complete")
    (.deleteOnExit package-file)
    package-file))

(defn get-package
  "Get a file with the package that can be uploaded to the nodes"
  [package]
  (cond
    (= "nil" package) nil
    (and (re-matches #".*\.rpm" package)
         (.isFile (io/file package))) {:type :rpm :package (io/file package)}
    (and (re-matches #".*\.deb" package)
         (.isFile (io/file package))) {:type :deb :package (io/file package)}
    (and (.isDirectory (io/file package))
         (.isDirectory (io/file package "bin"))
         (.isDirectory (io/file package "etc"))
         (.isDirectory (io/file package "lib"))
         (.isDirectory (io/file package "share"))) {:type    :tar
                                                    :package (io/file package)
                                                    :path    (.getCanonicalPath (io/file package))}
    :else (throw (RuntimeException. (str "Couldn't load package " package)))))

(defn get-remote-logs
  "Get a vector of log file paths"
  [testData]
  (let [install-dir (:install-path testData)]
    (c/su (c/exec :rm :-rf "/tmp/jepsen-logs")
          (c/exec :mkdir :-m777 "/tmp/jepsen-logs")
          ;; This file is required to ensure consistent behaviour. We want
          ;; Jepsen to detect /tmp/jepsen-logs as the root log directory.
          ;; Otherwise, the root would some times be /tmp/jepsen-logs and
          ;; sometime the extracted cbcollect directory.
          (c/exec :touch "/tmp/jepsen-logs/.ignore"))
    (when (:cbcollect testData)
      (info "Running cbcollect_info on" c/*host*)
      (c/su (c/exec (str install-dir "/bin/cbcollect_info") "/tmp/jepsen_cbcollect.zip")
            (c/exec :unzip "/tmp/jepsen_cbcollect.zip" :-d "/tmp/jepsen-logs")
            (c/exec :rm "/tmp/jepsen_cbcollect.zip")))
    (when (:hashdump testData)
      (info "Creating hashtable dump on" c/*host*)
      (let [vbuckets (format "$(seq 0 %d)" (:custom-vbucket-count testData 1024))
            cbstats (str install-dir "/bin/cbstats")
            hashcmd (str cbstats " localhost -u Administrator -p abc123 -b default raw \"_hash-dump $i\"")
            logfile "/tmp/jepsen-logs/hashtable_dump.txt"
            loopcmd (format "for i in %s; do (%s; echo) &>> %s; done" vbuckets hashcmd logfile)]
        (c/su (c/exec* loopcmd))))
    (when (:collect-data-files testData)
      (info "Collect Couchbase Servers data files")
      (c/su (c/exec* (str "zip -r /tmp/jepsen-logs/data-files.zip " (str (str (:install-path testData) "/var/lib/couchbase/data"))))))
    (when (:collect-core-files testData)
      (info "Collect core dump files")
      ;; zip returns non-zero status (12) if no files match, in which case just treat as success
      (c/su (c/exec* (str "zip /tmp/jepsen-logs/core-files.zip /tmp/core.* || [[ $? == 12 ]]"))))
    (when (:enable-tcp-capture testData)
      (info "Collecting tcp packet capture")
      (c/su (c/exec* (str "if [[ -f \"/var/run/daemonlogger.pid\" ]]; then kill -s TERM $(cat /var/run/daemonlogger.pid) || true; fi"))
            ;; clean up old gz files, if any
            (c/exec* (str "rm -f /packet-capture/*.gz*"))
            (c/exec* (str "gzip -f /packet-capture/*.pcap*"))
            (c/exec* (str "mv /packet-capture/*.gz* /tmp/jepsen-logs/"))))
    (c/su (c/exec :chmod :a+r :-R "/tmp/jepsen-logs"))
    (str/split-lines (c/exec :find "/tmp/jepsen-logs" :-type :f))))

(defn get-cluster-run-logs
  "Collect logs for all nodes"
  [testData node]
  (let [install-path (:install-path testData)
        cbcollect_info (.getCanonicalPath (io/file install-path "bin" "cbcollect_info"))
        initargs_file (.getCanonicalPath (io/file install-path
                                                  "../ns_server/data"
                                                  (-> (get-node-name node)
                                                      (str/split #"@")
                                                      (first))
                                                  "initargs"))
        tmp-collect (.getCanonicalPath (store/path testData (str "cbcollect-tmp-" node ".zip")))
        extract-path (.getCanonicalPath (store/path testData node))]
    (when (:cbcollect testData)
      (info "Running cbcollect_info on" node)
      (shell/sh cbcollect_info
                (str "--initargs=" initargs_file)
                tmp-collect)
      (shell/sh "unzip" tmp-collect "-d" extract-path)
      (shell/sh "rm" tmp-collect))
    (when (:hashdump testData)
      (let [vbuckets (format "$(seq 0 %d)" (or (:custom-vbucket-count testData) 1024))
            cbstats (str install-path "/bin/cbstats")
            nodestr (get-connection-string node)
            hashcmd (str cbstats " " nodestr " -u Administrator -p abc123 -b default raw \"_hash-dump $i\"")
            logfile (.getCanonicalPath (store/path testData node "hashtable_dump.txt"))
            ;; OSX ships with an ancient version of bash that doesn't support &>>
            loopcmd (format "for i in %s; do (%s; echo) >> %s 2>&1 ; done" vbuckets hashcmd logfile)]
        (shell/sh "bash" "-c" loopcmd)))
    nil))

(defn get-autofailover-info
  [target field]
  (let [autofailover-info (rest-call :get "/settings/autoFailover" {:target target})
        json-val (json/parse-string autofailover-info true)
        field-val (json-val (keyword field))]
    field-val))

(defn get-cluster-info
  [target]
  (let [rest-call (rest-call :get "/pools/default" {:target target})
        json-val (json/parse-string rest-call true)]
    json-val))

(defn get-node-info
  [target]
  (let [cluster-info (get-cluster-info target)
        nodes-vec (:nodes cluster-info)]

    (loop [node-info-map {}
           nodes-info nodes-vec]
      (if (not-empty nodes-info)
        (let [node-info (first nodes-info)
              otp-node (:otpNode node-info)
              node-name (get-node-id otp-node)
              updated-node-info-map (assoc node-info-map node-name node-info)
              updated-nodes-info (remove #(= node-info %) nodes-info)]
          (recur updated-node-info-map updated-nodes-info))
        node-info-map))))

(defn get-node-info-map
  "Construct a map that maps a node to the returned status information
  for each node from which we got status information. Note that is
  might be expected that some nodes don't return any data, for example
  if they have been removed from the cluster."
  [test]
  (into {} (keep (fn get-node-info-wrapper [target]
                   (domTop/with-retry [retries 5]
                     [target (get-node-info target)]
                     (catch Exception e
                       (if (pos? retries)
                         (retry (dec retries))))))
                 (:nodes test))))

(defn get-cluster-nodes
  "Get a list of nodes currently in the cluster. Note that if different nodes
  disagree on the current state of the cluster due to some failure condition,
  this function will likely fail."
  [test]
  (let [status-maps (get-node-info-map test)
        node-lists (map #(sort (keys %)) (vals status-maps))]
    (if (empty? node-lists)
      (throw (ex-info "Couldn't retrieve cluster data from any node." {:node-lists node-lists})))
    (if-not (apply = node-lists)
      (throw (ex-info "Cluster status data inconsistent between nodes." {:node-lists node-lists})))
    (first node-lists)))

(defn random-durability-level
  "Get a random durability level following the probability distribution in (:durability opts)"
  [durability]
  (let [rand-seed  (rand 100)]
    (->> (reductions + durability)
         (keep-indexed #(if (<= rand-seed %2) %1))
         (first))))

(defn start-cluster-run
  "Start a cluster-run for this test, destroying any currently existing runs"
  [test]
  (let [install-path (:install-path test)
        ns-server-dir (io/file install-path ".." "ns_server")
        config-data-dir (io/file ns-server-dir "data")
        cluster-run (io/file ns-server-dir "cluster_run")
        ns-server-log-dir (io/file ns-server-dir "logs")]
    (assert (.exists cluster-run) "Couldn't find cluster-run script in install")
    (info "Deleting data directory")
    (shell/sh "rm" "-rf" (.getPath config-data-dir))
    (info "Deleting log directory")
    (shell/sh "rm" "-rf" (.getPath ns-server-log-dir))
    (shell/sh "mkdir" (.getPath config-data-dir))
    (info "Starting cluster run")
    (let [ret (future (shell/sh (.getPath cluster-run)
                                (str "--nodes=" (:node-count test))
                                "--dont-rename"
                                :dir (.getPath ns-server-dir)))]
      (wait-for-daemon)
      ret)))
