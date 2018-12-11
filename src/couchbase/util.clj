(ns couchbase.util
  (:require [clojure.java.shell :as shell]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import com.couchbase.client.java.CouchbaseCluster
           com.couchbase.client.java.auth.ClassicAuthenticator
           com.couchbase.client.java.auth.PasswordAuthenticator))

(defn rest-call
  "Perform a rest api call"
  ([endpoint params] (rest-call c/*host* endpoint params))
  ([target endpoint params]
   (let [uri  (str "http://" target ":8091" endpoint)
         call (if (some? params)
                (shell/sh "curl" "-s" "-u" "Administrator:abc123" uri "-d" params)
                (shell/sh "curl" "-s" "-u" "Administrator:abc123" uri))]
     (if (not= (call :exit) 0)
       (throw+ {:type :rest-fail
                :target target
                :endpoint endpoint
                :params params
                :error call})
       (:out call)))))

(defn initialise
  "Initialise a new cluster"
  []
  (let [data-path  "%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata"
        index-path "%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata"
        params     (format "data_path=%s&index_path=%s" data-path index-path)]
    (rest-call "/nodes/self/controller/settings" params)
    (rest-call "/nodes/controller/setupServices" "services=kv")
    (rest-call "/settings/web" "username=Administrator&password=abc123&port=SAME")))

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

(defn setup-cluster
  "Setup couchbase cluster"
  [test node]
  (info "Creating couchbase cluster from" node)
  (let [nodes        (test :nodes)
        other-nodes  (remove #(= node %) nodes)
        num-vbuckets (test :custom-vbucket-count)
        num-replicas (test :replicas)]
    (initialise)
    (add-nodes other-nodes)
    (if num-vbuckets
      (->> num-vbuckets
           (format "ns_config:set(couchbase_num_vbuckets_default, %s).")
           (rest-call "/diag/eval")))
    (rebalance nodes)
    (create-bucket num-replicas)
    (rebalance nodes))
  (rest-call "/settings/autoFailover"
             (->> (test :autofailover)
                  (boolean)
                  (format "enabled=%s&timeout=6&maxCount=2")))
  (when-let [[lower_mark upper_mark] (test :custom-cursor-drop-marks)]
    (doseq [node (test :nodes)]
      (let [config (format "cursor_dropping_lower_mark=%d;cursor_dropping_upper_mark=%d"
                           lower_mark
			   upper_mark)
	    props  (format "[{extra_config_string, \"%s\"}]" config)
            params (format "ns_bucket:update_bucket_props(\"default\", %s)." props)]
	(rest-call node "/diag/eval" params)))
    (c/with-test-nodes test (c/su (c/exec :pkill :memcached)))
    (Thread/sleep 20000))
  (info "Setup complete"))

(defn setup-node
  "Start couchbase on a node"
  []
  (info "Setting up couchbase")
  (c/su (c/exec :mkdir "/opt/couchbase/var/lib/couchbase"))
  (c/su (c/exec :chown :couchbase:couchbase "/opt/couchbase/var/lib/couchbase"))
  (c/su (c/exec :service :couchbase-server :start))
  (info "Waiting for couchbase to start")
  (while
    (= :not-ready
      (try+
        (rest-call "/pools/default" nil)
        (catch [:type :rest-fail] e
          :not-ready)))
    (Thread/sleep 2000)))

(defn teardown
  "Stop the couchbase server instances and delete the data files"
  []
  (info "Tearing down couchbase node")
  (c/su (c/exec :service :couchbase-server :stop))
  (c/su (c/exec :rm :-rf "/opt/couchbase/var/lib/couchbase")))
  
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
