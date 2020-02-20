(ns couchbase.checker
  (:require [clojure.core.reducers :as r]
            [clojure.core :as c]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn error fatal]]
            [jepsen.checker :as checker]
            [jepsen.util :as util]
            [knossos.op :as op]))

(defn sanity-check
  "Return unknown validity if the test is broken."
  []
  (reify checker/Checker
    (check [this testData history opts]
      (let [reads   (->> history (filter #(and (= (:f %) :read) (= (:type %) :invoke))) (count))
            all-fail? (fn [ftype] (> (->> history
                                          (filter #(and (= (:f %) ftype) (= (:type %) :invoke)))
                                          (count)
                                          (* 0.01))
                                     (->> history
                                          (filter #(and (= (:f %) ftype) (= (:type %) :ok)))
                                          (count))))
            aborted (= @(:control-atom testData) :abort)]
        (cond
          aborted {:valid? :unknown :error "Test aborted"}
          (all-fail? :read) {:valid? :unknown :error "Insufficient read ops returned :ok"}
          (all-fail? :write) {:valid? :unknown :error "Insufficient write ops returned :ok"}
          (all-fail? :add) {:valid? :unknown :error "Insufficient add ops returned :ok"}
          :else   {:valid? true})))))

(defn set-upsert-checker
  "Given a set of :add operations followed by a final :read, verifies that
 1. every successfully upserted or added set is present in the read.
   a. If upsert for a set was not successful, then read for that set should
       have the corresponding set-add's value.
   b. If add for a set was not successful, then read for that set should
       have the corresponding upsert's value.
 2. and that the read contains only elements for which an add or upsert was attempted."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [add-attempts (->> history
                              (r/filter op/invoke?)
                              (r/filter #(= :add (:f %)))
                              (r/map :value)
                              (into #{}))
            upsert-attempts (->> history
                                 (r/filter op/invoke?)
                                 (r/filter #(= :upsert (:f %)))
                                 (r/map :value)
                                 (into #{}))

            add-ok (->> history
                        (r/filter op/ok?)
                        (r/filter #(= :add (:f %)))
                        (r/map :value)
                        (into #{}))

            upsert-ok (->> history
                           (r/filter op/ok?)
                           (r/filter #(= :upsert (:f %)))
                           (r/map :value)
                           (into #{}))

            final-read (into (hash-map) (->> history
                                             (r/filter op/ok?)
                                             (r/filter #(= :read (:f %)))
                                             (r/map :value)
                                             (reduce (fn [_ x] x) nil)))

            final-read-keys (into (sorted-set) (keys final-read))

            add-ok-values (into (sorted-set) add-ok)

            upsert-ok-values (->> history
                                  (r/filter op/ok?)
                                  (r/filter #(= :upsert (:f %)))
                                  (r/map :insert-value)
                                  (into (sorted-set)))]

        (if-not final-read-keys
          {:valid? :unknown
           :error  "Set was never read"}

          (let [final-read-keys (c/set final-read-keys)

                ; The OK set is every read value which we tried to add
                ok          (set/intersection final-read-keys add-attempts)

                ; Unexpected records are those we *never* attempted.
                unexpected  (set/difference final-read-keys (set/union add-attempts upsert-attempts))

                ; Lost records are those we definitely added or upserted but weren't read
                lost        (set/difference (set/union add-ok upsert-ok) final-read-keys)

                ;upsert-lost are those we definitely added but weren't upserted
                upsert-lost (set/difference add-ok upsert-ok)

                ;add-lost are those we definitely upserted but weren't added
                add-lost     (set/difference upsert-ok add-ok)

                ;;Its ok for upserts to be lost as long as the corresponding values of those keys were read from add operation.
                upsert-not-rolled-back          (set (remove nil? (map (fn [x] (if-not (contains? add-ok-values (get final-read x))
                                                                                 x)) upsert-lost)))
                ;;Its ok for adds to be lost as long as the corresponding values of those keys were read from upsert operation
                add-not-rolled-back             (set (remove nil? (map (fn [x] (if-not (contains? upsert-ok-values (get final-read x))
                                                                                 x)) add-lost)))

                upsert-rolled-back  (set/difference upsert-lost upsert-not-rolled-back)
                add-rolled-back     (set/difference add-lost add-not-rolled-back)


                ; Recovered records are those where we didn't know if the add
                ; succeeded or not, but we found them in the final set.


                recovered   (set/difference ok add-ok)]

            {:valid?              (and (empty? lost) (empty? unexpected) (empty? upsert-not-rolled-back) (empty? add-not-rolled-back))
             :add-attempt-count     (count add-attempts)
             :upsert-attempt-count (count upsert-attempts)
             :add-acknowledged-count  (count add-ok)
             :upsert-acknowledged-count  (count upsert-ok)
             :ok-count            (count ok)
             :lost-count          (count lost)
             :recovered-count     (count recovered)
             :unexpected-count    (count unexpected)
             :upsert-not-rolled-back-count  (count upsert-not-rolled-back)
             :add-not-rolled-back-count     (count add-not-rolled-back)
             :ok                  (util/integer-interval-set-str ok)
             :lost                (util/integer-interval-set-str lost)
             :unexpected          (util/integer-interval-set-str unexpected)
             :upsert-not-rolled-back  (util/integer-interval-set-str upsert-not-rolled-back)
             :add-not-rolled-back (util/integer-interval-set-str add-not-rolled-back)
             :upsert-rolled-back   (util/integer-interval-set-str upsert-rolled-back)
             :add-rolled-back  (util/integer-interval-set-str add-rolled-back)
             :recovered           (util/integer-interval-set-str recovered)}))))))

(defn extended-set-checker
  "Checker for operations over a set. A given key must have exactly one add
  operation, followed at most one delete. Cases involving multiple deletes,
  multiple adds, or deleting before adding are not considered, and thus will
  likely cause incorrect results. There should be a single successful read as
  the final operation."
  []
  (reify checker/Checker
    (check [this testData history opts]
      (let [ops         (group-by #(str (name (:f %)) (name (:type %))) history)
            add-invoke  (->> (ops "addinvoke")  (r/map :value) (into #{}))
            add-ok      (->> (ops "addok")      (r/map :value) (into #{}))
            add-info    (->> (ops "addinfo")    (r/map :value) (into #{}))
            add-fail    (->> (ops "addfail")    (r/map :value) (into #{}))
            del-invoke  (->> (ops "delinvoke")  (r/map :value) (into #{}))
            del-ok      (->> (ops "delok")      (r/map :value) (into #{}))
            del-info    (->> (ops "delinfo")    (r/map :value) (into #{}))
            del-fail    (->> (ops "delfail")    (r/map :value) (into #{}))
            read-invoke (->> (ops "readinvoke") (into #{}) (sort-by :time))
            read-ok     (->> (ops "readok")     (into #{}) (sort-by :time))]

        (if (> 1 (count read-ok))
          (warn "Multiple reads found, discarding all but last"))
        (if (->> (take-last 2 history)
                 (map #(str (name (:f %)) (name (:type %))))
                 (not= '("readinvoke" "readok")))
          (warn "Final two history entries were not invocation and success of read,"
                "results may be invalid"))

        (if-not (pos? (count read-ok))
          {:valid? :unknown :error "Set was never read"}

          (let [final-read (set (:value (last read-ok)))

                ;; Keys for which we never invoked delete
                no-delete-attempt (set/difference add-invoke del-invoke)

                ;; All keys we definitely didnt delete
                not-deleted (set/union no-delete-attempt del-fail)

                ;; Keys that must be present in the final read: ie the keys we
                ;; definitely created and definitely weren't deleted
                required-keys (set/intersection add-ok not-deleted)

                ;; There are two cases where the keys may or may not be present
                ;; in the read, either
                ;; 1) The add was confirmed ok, but the delete was indeterminate
                ;; 2) The add was indeterminate, and there was no successful delete
                permitted-keys (set/union (set/intersection add-ok del-info)
                                          (set/difference add-info del-ok))

                ;; Keys that were potentially added that we didn't read
                not-read (set/difference add-invoke final-read)

                ;; Keys we added and didn't delete, but weren't present in the read
                lost (set/difference required-keys final-read)

                ;; Keys are unexpected iff they are present in the read but
                ;; - We definitely deleted them, or
                ;; - We never attempted to add them, or
                ;; - We know the add definitely failed
                unexpected (set/union (set/intersection final-read del-ok)
                                      (set/difference   final-read add-invoke)
                                      (set/intersection final-read add-fail))

                ;; Keys are ok iff their state meets our expectations, ie they
                ;; - They are present in the read, and we required them to be, or
                ;; - They are not present in the read, and we required them to not be, or
                ;; - They are permitted to be present or not present.
                ;; This is equal to the keys that are neither lost nor unexpected
                ok (set/difference add-invoke (set/union lost unexpected))

                ;; Keys whos status was unsure, until we found them in the read
                recovered (set/intersection permitted-keys final-read)

                ;; Keys whos status was unsure, until we didn't find them in the read
                not-recovered (set/difference permitted-keys final-read)]

            (array-map
             :valid?              (and (empty? lost) (empty? unexpected))
             :add-count           (count add-invoke)
             :ok-count            (count ok)
             :lost-count          (count lost)
             :unexpected-count    (count unexpected)
             :indeterminate-count (count permitted-keys)
             :recovered-count     (count recovered)
             :not-recovered-count (count not-recovered)
             :ok                  (util/integer-interval-set-str ok)
             :lost                (util/integer-interval-set-str lost)
             :unexpected          (util/integer-interval-set-str unexpected)
             :recovered           (util/integer-interval-set-str recovered)
             :not-recovered       (util/integer-interval-set-str not-recovered))))))))

(defn sanity-counter
  "Checker for a counter workload to make sure that the end result is equal to
  the sum of add (negative or positive) ops performed on a counter. This will
  also return valid in a situation where the we have ambiguous ops that have
  succeeded"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [okayOps (filter #(and (op/ok? %) (= (:f %) :add)) history)
            opVals (map :value okayOps)
            counterSum (apply + opVals)
            lastCurrVal (:current-value (last okayOps))
            counterSumWithStartVal (+ counterSum (:init-counter-value test))

            reads (filter #(and (op/ok? %) (= (:f %) :read)) history)
            readsVals (map :value reads)

            ambiguousOps (filter #(and (op/info? %) (= (:f %) :add)) history)
            ambiguousOpsVals (map :value ambiguousOps)
            ambiguousOpsSum (apply + ambiguousOpsVals)

            failedOps (filter #(and (op/fail? %) (= (:f %) :add)) history)

            sum-of-neg-ambig-vals (apply + (filter neg? ambiguousOpsVals))
            sum-of-pos-ambig-vals (apply + (filter pos? ambiguousOpsVals))
            min-range (+ counterSumWithStartVal sum-of-neg-ambig-vals)
            max-range (+ counterSumWithStartVal sum-of-pos-ambig-vals)
            resultMap {:valid?                        (cond (= counterSumWithStartVal lastCurrVal) true
                                                            (and (>= lastCurrVal min-range)
                                                                 (<= lastCurrVal max-range)) (do (error "End values are not equal but end sum is in ambiguous range")
                                                                                                 :unknown)
                                                            :else false)
                       :min-range                     min-range
                       :max-range                     max-range
                       :starting-value                (:init-counter-value test)
                       :attempt-count                 (+ (count okayOps) (count ambiguousOps))
                       :ok-count                      (count okayOps)
                       :ambiguous-count               (count ambiguousOps)
                       :fail-count                    (count failedOps)
                       :summed-ops                    counterSumWithStartVal
                       :summed-ambiguous-ops          ambiguousOpsSum
                       :summed-negative-ambiguous-ops sum-of-neg-ambig-vals
                       :summed-positive-ambiguous-ops sum-of-pos-ambig-vals
                       :last-value                    lastCurrVal
                       :reads                         readsVals}]
        resultMap))))