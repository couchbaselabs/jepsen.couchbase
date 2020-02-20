(ns couchbase.checker_test
  (:refer-clojure :exclude [set])
  (:use clojure.test)
  (:require [jepsen.checker :as checker]
            [couchbase.checker :refer :all]))

(deftest set-upsert-checker-test

  ;;In the history we have 'value' to denote the set (to be added or upserted), and
  ;;insert-value to denote the value to be added(or upserted) to that set.

  (testing "add-roll-backs"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 3, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value 2, :time 4, :process 3,  :type :invoke, :f :add, :insert-value 2}
                           {:value 2, :time 5, :process 3,  :type :ok, :f :add, :insert-value 2}
                           {:value 2, :time 6, :process 1,  :type :invoke, :f :upsert, :insert-value 3}
                           {:value 2, :time 7, :process 1,  :type :ok, :f :upsert, :insert-value 3}
                           {:value 3, :time 8, :process 2,  :type :invoke, :f :add, :insert-value 3}
                           {:value 3, :time 9, :process 2,  :type :ok, :f :add, :insert-value 3}
                           {:value 3, :time 10, :process 3,  :type :invoke, :f :upsert, :insert-value 4}
                           {:value 3, :time 11, :process 3,  :type :ok, :f :upsert, :insert-value 4}
                           {:value nil, :time 12, :process 1,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3] [3 4]], :time 13, :process 1,  :type :ok, :f :read}]
                          ;;key 1 was not committed for add operation. So read for key 3 should give
                          ;;it's upserted value which is 4.
                          {})
           {:valid?              true
            :add-attempt-count     3
            :upsert-attempt-count 3
            :add-acknowledged-count  2
            :upsert-acknowledged-count  3
            :ok-count            3
            :lost-count          0
            :recovered-count     1
            :unexpected-count    0
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1..3}"
            :lost                "#{}"
            :unexpected           "#{}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{}"
            :add-rolled-back   "#{1}"
            :recovered            "#{1}"})))

  (testing "upsert-roll-backs"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 1,  :type :ok, :f :add, :insert-value 1}
                           {:value 1, :time 3, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 4, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value 2, :time 5, :process 3,  :type :invoke, :f :add, :insert-value 2}
                           {:value 2, :time 6, :process 3,  :type :ok, :f :add, :insert-value 2}
                           {:value 2, :time 7, :process 1,  :type :invoke, :f :upsert, :insert-value 3}
                           {:value 2, :time 8, :process 1,  :type :ok, :f :upsert, :insert-value 3}
                           {:value 3, :time 9, :process 2,  :type :invoke, :f :add, :insert-value 3}
                           {:value 3, :time 10, :process 2,  :type :ok, :f :add, :insert-value 3}
                           {:value 3, :time 11, :process 3,  :type :invoke, :f :upsert, :insert-value 4}
                           {:value nil, :time 12, :process 1,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3] [3 3]], :time 13, :process 1,  :type :ok, :f :read}]
                          ;;key 3 was not committed when upserted. So read for key 3 should be the insert-value of
                          ;;add operation which is 3.
                          {})
           {:valid?              true
            :add-attempt-count     3
            :upsert-attempt-count 3
            :add-acknowledged-count  3
            :upsert-acknowledged-count  2
            :ok-count            3
            :lost-count          0
            :recovered-count     0
            :unexpected-count    0
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1..3}"
            :lost                "#{}"
            :unexpected           "#{}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{3}"
            :add-rolled-back   "#{}"
            :recovered            "#{}"})))

  (testing "lost"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 1,  :type :ok, :f :add, :insert-value 1}
                           {:value 1, :time 3, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 4, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value 2, :time 5, :process 3,  :type :invoke, :f :add, :insert-value 2}
                           {:value 2, :time 6, :process 3,  :type :ok, :f :add, :insert-value 2}
                           {:value 2, :time 7, :process 1,  :type :invoke, :f :upsert, :insert-value 3}
                           {:value 2, :time 8, :process 1,  :type :ok, :f :upsert, :insert-value 3}
                           {:value 3, :time 9, :process 2,  :type :invoke, :f :add, :insert-value 3}
                           {:value 3, :time 10, :process 2,  :type :ok, :f :add, :insert-value 3}
                           {:value 3, :time 11, :process 3,  :type :invoke, :f :upsert, :insert-value 4}
                           {:value 3, :time 12, :process 3,  :type :ok, :f :upsert, :insert-value 4}
                           {:value nil, :time 13, :process 1,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3]], :time 14, :process 1,  :type :ok, :f :read}]
                          ;;If key 3 is not read even though it was both added and upserted
                          {})
           {:valid?              false
            :add-attempt-count     3
            :upsert-attempt-count 3
            :add-acknowledged-count  3
            :upsert-acknowledged-count  3
            :ok-count            2
            :lost-count          1
            :recovered-count     0
            :unexpected-count    0
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1..2}"
            :lost                "#{3}"
            :unexpected           "#{}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{}"
            :add-rolled-back   "#{}"
            :recovered            "#{}"})))

  (testing "unexpected-reads"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 1,  :type :ok, :f :add, :insert-value 1}
                           {:value 1, :time 3, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 4, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value nil, :time 5, :process 3,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3]], :time 6, :process 3,  :type :ok, :f :read}]
                          ;;If key 2 was read even though it was not added or upserted
                          {})
           {:valid?              false
            :add-attempt-count     1
            :upsert-attempt-count 1
            :add-acknowledged-count  1
            :upsert-acknowledged-count  1
            :ok-count            1
            :lost-count          0
            :recovered-count     0
            :unexpected-count    1
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1}"
            :lost                "#{}"
            :unexpected           "#{2}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{}"
            :add-rolled-back   "#{}"
            :recovered            "#{}"}))))
