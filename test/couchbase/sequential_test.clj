(ns couchbase.sequential_test
  (:require [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.test :refer [deftest testing is]]
            [couchbase.seqchecker :as seqchecker]
            [jepsen.checker :as checker]))

(deftest seqchecker_test
  (testing "sequential-ok"
    ;; Ensure a sequentially consistent history is accepted despite being
    ;; non-linearizable
    (is (= true
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                    {:f :write :type :ok :value 1 :process 1 :index 2}
                                    {:f :write :type :invoke :value 2 :process 1 :index 3}
                                    {:f :write :type :ok :value 2 :process 1 :index 4}
                                    {:f :read :type :invoke :value nil :process 2 :index 5}
                                    {:f :read :type :ok :value 1 :process 2 :index 6}
                                    {:f :read :type :invoke :value nil :process 2 :index 7}
                                    {:f :read :type :ok :value 2 :process 2 :index 8}
                                    {:f :read :type :invoke :value nil :process 3 :index 9}
                                    {:f :read :type :ok :value 1 :process 3 :index 10}]
                                   nil)))))

  ;; Sequential Consistency implies Monotonic Reads. Ensure a history with
  ;; non-monotonic reads is detected as not sequentially consistent.
  (testing "sequential-monotonic-read-fail"
    (is (= false
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                    {:f :write :type :ok :value 1 :process 1 :index 2}
                                    {:f :write :type :invoke :value 2 :process 1 :index 3}
                                    {:f :write :type :ok :value 2 :process 1 :index 4}
                                    {:f :read :type :invoke :value nil :process 2 :index 5}
                                    {:f :read :type :ok :value 1 :process 2 :index 6}
                                    {:f :read :type :invoke :value nil :process 2 :index 7}
                                    {:f :read :type :ok :value 2 :process 2 :index 8}
                                    {:f :read :type :invoke :value nil :process 2 :index 9}
                                    {:f :read :type :ok :value 1 :process 2 :index 10}]
                                   nil)))))

  ;; Sequential Consistency implies Monotonic Writes. Ensure a history with
  ;; non-monotonic writes is detected as not sequentially consistent.
  (testing "sequential-monotonic-write-fail"
    (is (= false
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                    {:f :write :type :ok :value 1 :process 1 :index 2}
                                    {:f :write :type :invoke :value 2 :process 1 :index 3}
                                    {:f :write :type :ok :value 2 :process 1 :index 4}
                                    {:f :read :type :invoke :value nil :process 2 :index 5}
                                    {:f :read :type :ok :value 2 :process 2 :index 6}
                                    {:f :read :type :invoke :value nil :process 2 :index 7}
                                    {:f :read :type :ok :value 1 :process 2 :index 8}]
                                   nil)))))

  ;; Sequential Consistency implies read-your-writes. Ensure a history which
  ;; violates read-your-writes condition is detected as not sequentially
  ;; consistent.
  (testing "sequential-ryw-fail"
    (is (= false
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                    {:f :write :type :ok :value 1 :process 1 :index 2}
                                    {:f :write :type :invoke :value 2 :process 1 :index 3}
                                    {:f :write :type :ok :value 2 :process 1 :index 4}
                                    {:f :read :type :invoke :value nil :process 1 :index 5}
                                    {:f :read :type :ok :value 1 :process 1 :index 6}]
                                   nil)))))

  ;; Ensure that history obeying PRAM consistency for which a single-order
  ;; exists is accepted as sequentially consistent.
  (testing "sequential-single-order-ok"
    (is (= true
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                    {:f :write :type :ok :value 1 :process 1 :index 2}
                                    {:f :write :type :invoke :value 2 :process 2 :index 3}
                                    {:f :write :type :ok :value 2 :process 2 :index 4}
                                    {:f :read :type :invoke :value nil :process 3 :index 5}
                                    {:f :read :type :ok :value 2 :process 3 :index 6}
                                    {:f :read :type :invoke :value nil :process 3 :index 7}
                                    {:f :read :type :ok :value 1 :process 3 :index 8}
                                    {:f :read :type :invoke :value nil :process 4 :index 9}
                                    {:f :read :type :ok :value 2 :process 4 :index 10}
                                    {:f :read :type :invoke :value nil :process 4 :index 11}
                                    {:f :read :type :ok :value 1 :process 4 :index 12}]
                                   nil)))))

  ;; Sequential Consistency implies single-order. Ensure a history for which
  ;; no single-order exists is detected as not sequentially consistent.
  (testing "sequential-single-order-fail"
    (is (= false
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                    {:f :write :type :ok :value 1 :process 1 :index 2}
                                    {:f :write :type :invoke :value 2 :process 2 :index 3}
                                    {:f :write :type :ok :value 2 :process 2 :index 4}
                                    {:f :read :type :invoke :value nil :process 3 :index 5}
                                    {:f :read :type :ok :value 2 :process 3 :index 6}
                                    {:f :read :type :invoke :value nil :process 3 :index 7}
                                    {:f :read :type :ok :value 1 :process 3 :index 8}
                                    {:f :read :type :invoke :value nil :process 4 :index 9}
                                    {:f :read :type :ok :value 1 :process 4 :index 10}
                                    {:f :read :type :invoke :value nil :process 4 :index 11}
                                    {:f :read :type :ok :value 2 :process 4 :index 12}]
                                   nil)))))

  ;; Test a real history that passes the linearizability checker (and thus must
  ;; also be sequentially consistent).
  (testing "linearizable-history"
    (is (= true
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   (->> "sample-history-linearizable.edn"
                                        (clojure.java.io/resource)
                                        (slurp)
                                        (string/split-lines)
                                        (map #(edn/read-string {:default (constantly nil)} %))
                                        (vec))
                                   nil)))))

  ;; Test a real history that fails the linearizability check, but has been
  ;; manually verified to be sequentially consistent.
  (testing "sequential-history"
    (is (= true
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   (->> "sample-history-sequential.edn"
                                        (clojure.java.io/resource)
                                        (slurp)
                                        (string/split-lines)
                                        (map #(edn/read-string {:default (constantly nil)} %))
                                        (vec))
                                   nil)))))

  ;; Test a real history that fails the linearizability check and has been
  ;; manually verified as not sequentially consistent either.
  (testing "non-sequential-history"
    (is (= false
           (:valid? (checker/check (seqchecker/sequential)
                                   nil
                                   (->> "sample-history-non-sequential.edn"
                                        (clojure.java.io/resource)
                                        (slurp)
                                        (string/split-lines)
                                        (map #(edn/read-string {:default (constantly nil)} %))
                                        (vec))
                                   nil)))))

  ;; Ensure an illegal history that contains client operations with an unknown
  ;; type (i.e. other than :ok, :info, :invoke, or :fail) cause an exception to
  ;; be thrown
  (testing "illegal-op-type"
    (is (thrown? clojure.lang.ExceptionInfo
                 (checker/check (seqchecker/sequential)
                                nil
                                [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                 {:f :write :type :something-strange :value 1 :process 1 :index 2}]
                                nil))))

  ;; Ensure an illegal history containg client operations other than reads or
  ;; writes causes an exception to be thrown
  (testing "illegal-op-function"
    (is (thrown? clojure.lang.ExceptionInfo
                 (checker/check (seqchecker/sequential)
                                nil
                                [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                 {:f :write :type :ok :value 1 :process 1 :index 2}
                                 {:f :cas :type :invoke :value [1 2] :process 1 :index 3}
                                 {:f :cas :type :ok :value [1 2] :process 1 :index 4}]
                                nil))))

  ;; Ensure an illegal history containing indeterminate reads causes an
  ;; exception to be thrown
  (testing "illegal-op-indeterminate-read"
    (is (thrown? clojure.lang.ExceptionInfo
                 (checker/check (seqchecker/sequential)
                                nil
                                [{:f :read :type :invoke :value nil :process 1 :index 1}
                                 {:f :read :type :info :value nil :process 1 :index 2}]
                                nil))))

  ;; Ensure an illegal history containing multiple write invocations with the
  ;; same value causes an exception to be thrown
  (testing "illegal-non-unique-write-value"
    (is (thrown? clojure.lang.ExceptionInfo
                 (checker/check (seqchecker/sequential)
                                nil
                                [{:f :write :type :invoke :value 1 :process 1 :index 1}
                                 {:f :write :type :fail :value 1 :process 1 :index 2}
                                 {:f :write :type :invoke :value 1 :process 1 :index 3}
                                 {:f :write :type :ok :value 1 :process 1 :index 4}]
                                nil)))))


