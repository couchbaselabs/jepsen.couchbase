(ns couchbase.util
  (:require [clojure.java.shell :as shell]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]]
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

(defn rebalance
  "Rebalance the cluster"
  [nodes]
  (let [known-nodes-str (->> nodes
                             (map #(str "ns_1@" %))
                             (str/join ",")
                             (str "knownNodes="))
        get-status      #(rest-call "/pools/default/rebalanceProgress" nil)]
    (rest-call "/controller/rebalance" known-nodes-str)

    (loop [status (get-status)]
      (if (not= status "{\"status\":\"none\"}")
        (do
          (info "Rebalance status" status)
          (try+
           (Thread/sleep 1000)
           (catch InterruptedException e
             (warn "Interrupted during rebalance")
             (->> (Thread/currentThread) (.interrupt))))
          (recur (get-status)))
        (info "Rebalance complete")))))

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
  (let [enabled  (boolean (test :autofailover))
        timeout  (or (test :autofailover-timeout) 6)
        maxcount (or (test :autofailover-maxcount) 3)]
    (rest-call "/settings/autoFailover"
               (format "enabled=%s&timeout=%s&maxCount=%s"
                       enabled timeout maxcount))))

(defn set-custom-cursor-drop-marks
  "Set the cursor dropping marks to a new value on all nodes"
  [test]
  (let [lower_mark (nth (test :custom-cursor-drop-marks) 0)
          upper_mark (nth (test :custom-cursor-drop-marks) 1)
          config     (format "cursor_dropping_lower_mark=%d;cursor_dropping_upper_mark=%d"
                             lower_mark
			     upper_mark)
	  props      (format "[{extra_config_string, \"%s\"}]" config)
          params     (format "ns_bucket:update_bucket_props(\"default\", %s)." props)]
      (doseq [node (test :nodes)]
        (rest-call "/diag/eval" params)))
  (c/with-test-nodes test (c/su (c/exec :pkill :memcached)))
  ;; Before polling to check if we have warmed up again, we need to wait a while
  ;; for ns_server to detect memcached was killed
  (Thread/sleep 3000)
  (info "Waiting for memcached to restart")
  (while (->> (rest-call "/pools/default/serverGroups" nil)
              (re-find #"\"status\":\"warmup\""))
    (Thread/sleep 1000)))

(defn setup-cluster
  "Setup couchbase cluster"
  [test node]
  (info "Creating couchbase cluster from" node)
  (let [nodes        (test :nodes)
        other-nodes  (remove #(= node %) nodes)
        num-replicas (test :replicas)]
    (initialise)
    (add-nodes other-nodes)
    (set-vbucket-count test)
    (rebalance nodes)
    (set-autofailover test)
    (create-bucket num-replicas)
    (info "Waiting for bucket warmup to complete...")
    (while (->> (rest-call "/pools/default/serverGroups" nil)
                (re-find #"\"status\":\"warmup\""))
      (Thread/sleep 1000))
    (if (test :custom-cursor-drop-marks)
      (set-custom-cursor-drop-marks test))
    (info "Setup complete")))

(defn setup-node
  "Start couchbase on a node"
  [test]
  (info "Setting up couchbase")
  (c/su (c/exec :mkdir :-p "/opt/couchbase/var/lib/couchbase"))
  (c/su (c/exec :chmod :a+rwx "/opt/couchbase/var/lib/couchbase"))
  (when-let [package (test :package)]
    (info "Uploading couchbase package to nodes")
    (c/su (c/upload (:package package) "couchbase-package"))
    (info "Installing package")
    (case (:type package)
      :rpm
      (do
        (c/su (c/exec :mv "~/couchbase-package" "~/couchbase.rpm"))
        (c/su (c/exec :yum :install :-y "~/couchbase.rpm"))
        (c/su (c/exec :rm "~/couchbase.rpm")))
      :deb
      (do
        (c/su (c/exec :mv "~/couchbase-package" "~/couchbase.deb"))
        (c/su (c/exec :apt :install :-y "~/couchbase.deb"))
        (c/su (c/exec :rm "~/couchbase.deb")))
      :build
      (do
        (c/su (c/upload (:package (test :package)) "couchbase-build.tar"))
        (c/su (c/exec :tar :-Pxf "~/couchbase-package"))
        (c/su (c/exec :rm "~/couchbase-package"))

        ;; In order for paths in the builds bin directory to be resolveable, all parent
        ;; directories need to have execute permissions set. This might be a security
        ;; risk, but with vagrants we dont really care
        (loop [dir-tree (str/split (:path (test :package)) #"/")]
          (c/su (c/exec :chmod :a+x (str/join "/" dir-tree)))
          (if (> (count dir-tree) 2)
            (recur (drop-last dir-tree))))

        ;; We need to make the data directory writable by couchbase
        (c/su (c/exec :chmod :-R :a+rwx (str (:path (test :package)) "/var/lib/couchbase")))

        ;; Install systemd file
        (let [service-file (File/createTempFile "couchbase" ".service")]
          (.deleteOnExit service-file)
          (io/copy (io/reader (io/resource "couchbase-server.service")) service-file)
          (c/su (c/upload service-file "/etc/systemd/system/couchbase-server.service")))
        (c/su (c/exec :sed :-i (format "s/%%INSTALLPATH%%/%s/"
                                       (str/escape (:path (test :package)) {\/ "\\/"}))
                      "/etc/systemd/system/couchbase-server.service")))))
  (c/su (c/exec :systemctl :daemon-reload))
  (c/su (c/exec :systemctl :unmask :couchbase-server))
  (c/su (c/exec :systemctl :restart :couchbase-server))
  (info "Waiting for couchbase to start")
  (while
    (= :not-ready
      (try+
        (rest-call "/pools/default" nil)
        (catch [:type :rest-fail] e
          (if (= (->> e (:error) (:exit)) 7)
            :not-ready
            :done))))
    (Thread/sleep 2000)))

(defn teardown
  "Stop the couchbase server instances and delete the data files"
  [test]
  (info "Tearing down couchbase node")
  (try
    (c/su (c/exec :systemctl :stop :couchbase-server))
    (catch RuntimeException e))
  (c/su (c/exec :rm :-rf "/opt/couchbase/var/lib/couchbase"))
  (when (test :package)
    (case (get-package-manager)
      :yum (try
             (c/su (c/exec :yum :remove :-y "couchbase-server"))
             (c/su (c/exec :rm "~/couchbase.rpm"))
             (catch RuntimeException e))
      :apt (try
             (c/su (c/exec :apt :remove :-y "couchbase-server"))
             (c/su (c/exec :rm "~/couchbase.deb"))
             (catch RuntimeException e))))
  (when (= (:type (test :package)) :build)
    (try
      (c/su (c/exec :rm "/lib/systemd/system/couchbase-server.service"))
      (c/su (c/exec :rm :-r (:path (test :package))))
      (catch RuntimeException e
        (warn "Error removing couchbase install" e)))))
  
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
  (let [nodes   (test :nodes)
        auth    (if (>= (first (get-version (first nodes))) 5)
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
         (.isFile (io/file package)))        {:type :rpm :package (io/file package)}
    (and (re-matches #".*\.deb" package)
         (.isFile (io/file package)))        {:type :deb :package (io/file package)}
    (and (.isDirectory (io/file package))
         (.isDirectory (io/file package "bin"))
         (.isDirectory (io/file package "etc"))
         (.isDirectory (io/file package "lib"))
         (.isDirectory (io/file package "share"))) {:type :build
                                                    :package (tar-build (io/file package))
                                                    :path (.getCanonicalPath (io/file package))}))
