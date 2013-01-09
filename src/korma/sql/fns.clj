(ns korma.sql.fns
  (:require [korma.sql.engine :as eng]
            [korma.sql.utils :as utils]
            [korma.db :as db])
  (:use [korma.sql.engine :only [infix group-with sql-func trinary wrapper]]))

;;*****************************************************
;; Predicates
;;*****************************************************


(defn pred-and [& args]
  (apply eng/pred-and args))

(defn pred-or [& args] (group-with " OR " args))
(defn pred-not [v] (wrapper "NOT" v))

(defn pred-in [k v]     (infix k "IN" v))
(defn pred-not-in [k v] (infix k "NOT IN" v))
(defn pred-> [k v]      (infix k ">" v))
(defn pred-< [k v]      (infix k "<" v))
(defn pred->= [k v]     (infix k ">=" v))
(defn pred-<= [k v]     (infix k "<=" v))
(defn pred-like [k v]   (infix k "LIKE" v))

(defn pred-between [k [v1 v2]]
  (trinary k "BETWEEN" v1 "AND" v2))

(def pred-= eng/pred-=)
(defn pred-not= [k v] (cond
                        (and k v) (infix k "!=" v)
                        k         (infix k "IS NOT" v)
                        v         (infix v "IS NOT" k)))

;;*****************************************************
;; Aggregates
;;*****************************************************
(defn subprotocol [query]
  (get-in query [:db :options :subprotocol]
          (get-in @db/_default [:options :subprotocol])))

(defmulti agg-count (fn [query v] (subprotocol query)))

(defmethod agg-count "mysql" [query v]
  "On MySQL, when an argument for COUNT() is a '*',
   it must be a simple '*', instead of 'fieldname.*'.
   We must change :* to '*' before calling sql-func.
   sql-func never put field-prefix to '*' if it isn't a keyword."
  (let [x (if (keyword? v) (name v) v)]
    (if (= x "*")
      (sql-func "COUNT" (utils/generated x))
      (sql-func "COUNT" v))))

(defmethod agg-count :default [query v]
  "Default implementation for other than MySQL.
   simplly call sql-func."
  (sql-func "COUNT" v))

(defn agg-sum [query v]   (sql-func "SUM" v))
(defn agg-avg [query v]   (sql-func "AVG" v))
(defn agg-min [query v]   (sql-func "MIN" v))
(defn agg-max [query v]   (sql-func "MAX" v))
(defn agg-first [query v] (sql-func "FIRST" v))
(defn agg-last [query v]  (sql-func "LAST" v))
