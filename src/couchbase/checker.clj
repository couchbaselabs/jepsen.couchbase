(ns couchbase.checker
  (:require [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jepsen.checker :as checker]
            [jepsen.util :as util]
            [knossos.op :as op]))

(defn extended-set-checker
  "Checker for operations over a set. A given key must have exactly one add
  operation, followed at most one delete. Cases involving multiple deletes,
  multiple adds, or deleting before adding are not considered, and thus will
  likely cause incorrect results. There should be a single successful read as
  the final operation."
  []
  (reify checker/Checker
    (check [this test model history opts]
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

        (if-not (> (count read-ok) 0)
          {:valid? :unknown :error "Set was never read"}

          (let [final-read (into #{} (:value (last read-ok)))

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
                                          (set/difference   add-info del-ok))

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
                ok2 (set/union (set/intersection final-read required-keys)
                               (set/intersection not-read del-ok)
                               permitted-keys)

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
