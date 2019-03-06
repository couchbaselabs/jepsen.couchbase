(ns couchbase.util
  (:require [clojure.java.shell :as shell]
            [clojure.set    :as set]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clj-http.client :as client]
            [cheshire.core :refer :all]
            [jepsen [control :as c]
                    [net     :as net]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import com.couchbase.client.java.CouchbaseCluster
           com.couchbase.client.java.auth.ClassicAuthenticator
           com.couchbase.client.java.auth.PasswordAuthenticator
           java.io.File))

(defn rest-call
  "Perform a rest api call"
  ([endpoint params] (rest-call c/*host* endpoint params))
  ([target endpoint params]
   (let [;; /diag/eval is only accessible from localhost on newer couchbase
         ;; versions, so if endpoint is /diag/eval ssh into the node before
         ;; calling curl
         uri  (if (= endpoint "/diag/eval")
                (str "http://localhost:8091"  endpoint)
                (str "http://" target ":8091" endpoint))
         cmd  (if (= endpoint "/diag/eval")
                (fn [& args] {:out  (apply c/exec args)
                              :exit 0})
                shell/sh)
         call (if (some? params)
                (cmd "curl" "-s" "-S" "--fail" "-u" "Administrator:abc123" uri "-d" params)
                (cmd "curl" "-s" "-S" "--fail" "-u" "Administrator:abc123" uri))]
     (if (not= (call :exit) 0)
       (throw+ {:type :rest-fail
                :target target
                :endpoint endpoint
                :params params
                :error call})
       (:out call)))))

(defn get-package-manager
  "Get the package manager for the nodes os, only really designed for determining
  between centos and ubuntu"
  []
  (if (= "yum" (c/exec :bash :-c "if [ -e /etc/redhat-release ]; then echo yum; fi"))
    :yum
    (if (= "apt" (c/exec :bash :-c "if [ -e /etc/debian_version ]; then echo apt; fi"))
      :apt
      (throw (RuntimeException. "Couldn't determine node os")))))

(defn initialise
  "Initialise a new cluster"
  []
  (let [data-path  "%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata"
        index-path "%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata"
        params     (format "data_path=%s&index_path=%s" data-path index-path)]
    (rest-call "/nodes/self/controller/settings" params)
    (rest-call "/node/controller/setupServices" "services=kv")
    (rest-call "/settings/web" "username=Administrator&password=abc123&port=SAME")
    (rest-call "/pools/default" "memoryQuota=256")))

(defn add-nodes
  "Add nodes to the cluster"
  [nodes-to-add]
  (doseq [node nodes-to-add]
    (let [params (str "hostname=" node
                      "&user=Administrator"
                      "&password=abc123"
                      "&services=kv")]
      (info "Adding node" node "to cluster")
      (rest-call "/controller/addNode" params))))

(defn wait-for
  [call-function desired-state]
  (loop [state (call-function)]
    (if (not= state desired-state)
      (do
        (Thread/sleep 1000)
        (recur (call-function))))))

(defn get-rebalance-status
  [target]
  (let [rebalance-info (rest-call target "/pools/default/rebalanceProgress" nil)
        rebalance-info-map (parse-string rebalance-info true)
        rebalance-status (:status rebalance-info-map)]
    rebalance-status))


(defn rebalance
  "Inititate a rebalance with the given parameters"
  ([known-nodes] (rebalance known-nodes nil))
  ([known-nodes eject-nodes]
   (let [known-nodes-str (->> known-nodes
                              (map #(str "ns_1@" %))
                              (str/join ","))
         eject-nodes-str (->> eject-nodes
                              (map #(str "ns_1@" %))
                              (str/join ","))
         params (format "ejectedNodes=%s&knownNodes=%s"
                        eject-nodes-str
                        known-nodes-str)
         rest-target (first (apply disj (set known-nodes) eject-nodes))]
     (if eject-nodes
       (info "Rebalancing nodes" eject-nodes "out of cluster"))
     (rest-call rest-target "/controller/rebalance" params)
     (loop [status ""]
       (when (not= status "{\"status\":\"none\"}")
         (info "Rebalance status:" status)
         (if (re-find #"Rebalance failed" status)
           (throw (RuntimeException. "Rebalance failed")))
         (Thread/sleep 1000)
         (recur (rest-call rest-target "/pools/default/rebalanceProgress" nil))))
       (info "Rebalance complete"))))

(defn create-bucket
  "Create the default bucket"
  [replicas]
  (let [params (str "flushEnabled=1&replicaNumber=" replicas
                    "&evictionPolicy=fullEviction&ramQuotaMB=100"
                    "&bucketType=couchbase&name=default"
                    "&authType=sasl&saslPassword=")]
    (rest-call "/pools/default/buckets" params)))

(defn set-vbucket-count
  "Set the number of vbuckets for new buckets"
  [test]
  (if-let [num-vbucket (test :custom-vbucket-count)]
    (rest-call "/diag/eval"
               (format "ns_config:set(couchbase_num_vbuckets_default, %s)."
                       num-vbucket))))

(defn set-autofailover
  "Apply autofailover settings to cluster"
  [test]
  (let [enabled (boolean (test :autofailover))
        sg-enabled (boolean (test :server-group-autofailover))
        timeout (or (test :autofailover-timeout) 6)
        maxcount (or (test :autofailover-maxcount) 3)]
    (rest-call "/settings/autoFailover"
               (format "enabled=%s&timeout=%s&maxCount=%s&failoverServerGroup=%s"
                       enabled timeout maxcount sg-enabled))))

(defn wait-for-warmup
  "Wait for warmup to complete"
  []
  (while (->> (rest-call "/pools/default" nil)
              (re-find #"\"status\":\"warmup\""))
    (Thread/sleep 1000)))

(defn set-custom-cursor-drop-marks
  "Set the cursor dropping marks to a new value on all nodes"
  [test]
  (let [lower_mark (nth (test :custom-cursor-drop-marks) 0)
        upper_mark (nth (test :custom-cursor-drop-marks) 1)
        config (format "cursor_dropping_lower_mark=%d;cursor_dropping_upper_mark=%d"
                       lower_mark
                       upper_mark)
        props (format "[{extra_config_string, \"%s\"}]" config)
        params (format "ns_bucket:update_bucket_props(\"default\", %s)." props)]
    (doseq [node (test :nodes)]
      (rest-call "/diag/eval" params)))
  (c/with-test-nodes test (c/su (c/exec :pkill :memcached)))
  ;; Before polling to check if we have warmed up again, we need to wait a while
  ;; for ns_server to detect memcached was killed
  (Thread/sleep 3000)
  (info "Waiting for memcached to restart")
  (wait-for-warmup))

(defn create-server-groups
  [server-group-count]
  (let [server-group-nums (vec (range 1 (+ server-group-count 1)))]
    (doseq [server-group-num server-group-nums]
      (if (not= server-group-num 1)
        (rest-call "/pools/default/serverGroups" (format "name=Group %s" server-group-num))))))

(defn populate-server-groups
  "This function will deterministically add nodes to server groups"
  [test]
  (let [server-group-info (rest-call "/pools/default/serverGroups" nil)
        server-group-json (parse-string server-group-info true)
        revision-uri (:uri server-group-json)
        server-groups (:groups server-group-json)
        nodes (atom #{}) ; this will be used to build up and store the set of nodes in the cluster
        groups (atom []) ; this will be used to build up and store the vector of group json
        endpoint (str "http://" (first (:nodes test)) ":8091" revision-uri)]
    ; accumulate nodes and groups into respective atoms
    (doseq [group server-groups]
      (reset! nodes (set/union @nodes (set (:nodes group))))
      (reset! groups (conj @groups (assoc group :nodes []))))
    ; sort groups for deterministic population
    (reset! groups (vec (sort-by :name @groups)))
    ; for each node we calculate the group it should belong to and
    ; add it to the nodes field in the group json in groups atom
    (doseq [index (range 0 (count @nodes))]
      (let [group-count (count @groups)
            group-index (mod index group-count)
            node-index (quot index group-count)
            node-to-add (first @nodes)
            current-group-nodes (vec (get-in @groups [group-index :nodes]))
            updated-group-nodes (vec (assoc current-group-nodes node-index node-to-add))
            updated-groups (assoc-in @groups [group-index :nodes] updated-group-nodes)]
        ; after adding the node to cooresponding group, we remove the node from nodes atom
        (reset! nodes (set (remove #{node-to-add} @nodes)))
        (reset! groups updated-groups)))
    (client/put endpoint
                 {:basic-auth ["Administrator" "abc123"]
                  :body (generate-string {:groups @groups})
                  :headers {"X-Api-Version" "2"}
                  :content-type :json
                  :socket-timeout 1000
                  :conn-timeout 1000
                  :accept :json})))

(defn setup-server-groups
  [test]
  (let [server-group-count (:server-group-count test)
        nodes (:nodes test)]
    (create-server-groups server-group-count)
    (populate-server-groups test)))

(defn setup-cluster
  "Setup couchbase cluster"
  [test node]
  (info "Creating couchbase cluster from" node)
  (let [nodes (test :nodes)
        other-nodes (remove #(= node %) nodes)
        num-replicas (test :replicas)]
    (initialise)
    (add-nodes other-nodes)
    (set-vbucket-count test)
    (rebalance nodes)
    (set-autofailover test)
    (create-bucket num-replicas)
    (info "Waiting for bucket warmup to complete...")
    (wait-for-warmup)
    (if (test :custom-cursor-drop-marks)
      (set-custom-cursor-drop-marks test))
    (if (and (:server-group-count test)
             (> (:server-group-count test) 1))
      (setup-server-groups test))
    (info "Setup complete")))

(defn install-package
  "Install the given package on the nodes"
  [package]
  (case (:type package)
    :rpm (do
           (let [package-name (.getName (:package package))
                 split-package-name (str/split package-name #"-")
                 package-version (str (nth split-package-name 3) "-" (nth split-package-name 4))]
             (try
               (do
                 (c/su (c/exec (str "/opt/couchbase" "/bin/couchbase-server") :-v))
                 (c/su (c/exec :rpm :-e "couchbase-server"))
                 (throw (Exception. "removed server")))
               (catch Exception e
                 (let [root-files (c/su (c/exec :ls "/root"))]
                   (if (not (str/includes? root-files package-name))
                     (c/su (c/upload (:package package) package-name)))
                   (c/su (c/exec :yum :install :-y (str "/root/" package-name)))
                   (c/su (c/exec :mv (str "/root/" package-name) "/tmp/"))
                   (c/su (c/exec :rm :-rf "/root/*"))
                   (c/su (c/exec :mv (str "/tmp/" package-name) "/root/")))))))
    :deb (do
           (c/su (c/upload (:package package) "couchbase.deb"))
           (c/su (c/exec :apt :install :-y "~/couchbase.deb"))
           (c/su (c/exec :rm "~/couchbase.deb")))
    :tar (do
           (c/su (c/upload (:package package) "couchbase.tar"))
           (c/su (c/exec :tar :-Pxf "~/couchbase.tar"))
           (c/su (c/exec :rm "~/couchbase.tar")))))

(defn wait-for-daemon
  "Wait until couchbase server daemon has started"
  []
  (while
    (= :not-ready
       (try+
         (rest-call "/pools/default" nil)
         (catch [:type :rest-fail] e
           (if (= (->> e (:error) (:exit)) 7)
             :not-ready
             :done))))
    (Thread/sleep 2000)))

(defn setup-node
  "Start couchbase on a node"
  [test]
  (info "Setting up couchbase")
  (let [package (:package test)
        path (or (:path package) "/opt/couchbase")]
    (c/su (c/exec :mkdir :-p (str path "/var/lib/couchbase")))
    (c/su (c/exec :chmod :a+rwx (str path "/var/lib/couchbase")))
    (if package
      (install-package package))
    (info "Starting daemon")
    (c/ssh* {:cmd (str "nohup " path "/bin/couchbase-server -- -noinput >> /dev/null 2>&1 &")}))
  (wait-for-daemon))

(defn teardown
  "Stop the couchbase server instances and delete the data files"
  [test]
  (if (and (test :skip-teardown) (deref (test :db-intialized)))
    (info "Skipping teardown of couchbase node")
    (do
      (info "Tearing down couchbase node")
      (try
        (c/su (c/exec :systemctl :stop :couchbase-server))
        (catch RuntimeException e))
      (try
        (let [path (or (:path (:package test))
                       "/opt/couchbase")]
          (c/su (c/exec (str path "/bin/couchbase-server") :-k)))
        (catch RuntimeException e))
      (c/su (c/exec :rm :-rf "/opt/couchbase/var/lib/couchbase"))
      (net/heal! (:net test) test)
      )
    )
  (info "Teardown Complete"))

(defn get-version
  "Get the couchbase version running on the cluster"
  [node]
  (->> (rest-call node "/pools/" nil)
       (re-find #"(?<=\"implementationVersion\":\")([0-9])\.([0-9])\.([0-9])")
       (rest)
       (map #(Integer/parseInt %))))

(defn get-connection
  "Use the couchbase java sdk to create a CouchbaseCluster instance, then open
  the default bucket."
  [test]
  (let [nodes (test :nodes)
        auth (if (or (>= (first (get-version (first nodes))) 5)
                     (= (first (get-version (first nodes))) 0))
               (new PasswordAuthenticator "Administrator" "abc123")
               (-> (new ClassicAuthenticator)
                   (.cluster "Administrator" "abc123")
                   (.bucket "default" "")))
        cluster (-> (CouchbaseCluster/create nodes)
                    (.authenticate auth))]
    cluster))

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
              (.getCanonicalPath package-file)
              "--owner=root"
              "--group=root"
              (.getCanonicalPath build))
    (info "TAR complete")
    (.deleteOnExit package-file)
    package-file))

(defn get-package
  "Get a file with the package that can be uploaded to the nodes"
  [package]
  (cond
    (and (re-matches #".*\.rpm" package)
         (.isFile (io/file package))) {:type :rpm :package (io/file package)}
    (and (re-matches #".*\.deb" package)
         (.isFile (io/file package))) {:type :deb :package (io/file package)}
    (and (.isDirectory (io/file package))
         (.isDirectory (io/file package "bin"))
         (.isDirectory (io/file package "etc"))
         (.isDirectory (io/file package "lib"))
         (.isDirectory (io/file package "share"))) {:type    :tar
                                                    :package (tar-build (io/file package))
                                                    :path    (.getCanonicalPath (io/file package))}
    :else (throw (RuntimeException. (str "Couldn't load package " package)))))

(defn get-logs
  "Get a vector of log file paths"
  [test]
  (let [install-dir (or (:path (test :package))
                        "/opt/couchbase")]
    (when (test :get-cbcollect)
      (info "Generating cbcollect...")
      (c/su (c/exec (str install-dir "/bin/cbcollect_info")
                    (str install-dir "/var/lib/couchbase/logs/cbcollect.zip"))))
    (when (test :hashdump)
      (info "Getting hashtable dump from all vbuckets")
      (c/su
        (c/exec
          :for :i :in (c/lit "$(seq 0 1023);") :do
          (str install-dir "/bin/cbstats")
          :localhost :-u :Administrator :-p :abc123 :-b :default :raw
          (c/lit "\"_hash-dump $i\"")
          :>> (str install-dir "/var/lib/couchbase/logs/hashdump.txt") (c/lit ";")
          :echo :>> (str install-dir "/var/lib/couchbase/logs/hashdump.txt") (c/lit ";")
          :done)))

    (c/su (c/exec :chmod :-R :a+rx "/opt/couchbase"))
    (try
      (->> (c/exec :ls (str install-dir "/var/lib/couchbase/logs"))
           (str/split-lines)
           (map #(str install-dir "/var/lib/couchbase/logs/" %)))
      (catch RuntimeException e
        (warn "Error getting logfiles")
        []))))

(defn get-autofailover-info
  [target field]
  (let [autofailover-info (rest-call target "/settings/autoFailover" nil)
        json-val (parse-string autofailover-info true)
        field-val (json-val (keyword field))]
    field-val))

(defn get-cluster-info
  [target]
  (let [rest-call (rest-call target "/pools/default" nil)
        json-val (parse-string rest-call true)]
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
              node-name (str/replace otp-node #"ns_1@" "")
              updated-node-info-map (assoc node-info-map node-name node-info)
              updated-nodes-info (remove #(= node-info %) nodes-info)]
          (recur updated-node-info-map updated-nodes-info))
        node-info-map))))