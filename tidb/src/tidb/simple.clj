(ns tidb.simple
  "A test for long time transaction"
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen
             [client :as client]
             [generator :as gen]
             [checker :as checker]]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [clojure.tools.logging :refer :all]))

(defrecord SimpleClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (locking SimpleClient
      (do
        (c/execute! conn ["create table if not exists test_simple (
                            age int not null primary key,
                            nickname varchar(20) not null,
                            gender int not null default 0,
                            first_name varchar(30) not null default '',
                            last_name varchar(20) not null default '',
                            full_name varchar(60) as (concat(first_name, ' ', last_name)),
                            index idx_nickname (nickname)
                          ) partition by range (age) (
                            partition child values less than (18),
                            partition young values less than (30),
                            partition middle values less than (50),
                            partition old values less than (123)
                          );"])

        (try
          (c/insert! conn :test_simple {:age 25
                                        :nickname "cosven"
                                        :first_name "sw"
                                        :last_name "y"})
          (catch java.sql.SQLIntegrityConstraintViolationException e nil)))))

  (invoke! [this test op]
    (case (:f op)
      :read
      (->>
       (c/query conn [(str "select * from test_simple")])
       (map (juxt :age :full_name))
       (into (sorted-map))
       (assoc op :type :ok, :value))

      :update
      (do
        (c/update! conn :test_simple
                   {:first_name (rand-nth ["sw" "cosven"])}
                   ["age = ?" "25"])
        (assoc op :type :ok :value {:update "first_name"}))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn read
  "A geenrator for loading fixture data."
  [_ _]
  {:type :invoke, :f :read})

(defn write
  "A geenrator for loading fixture data."
  [_ _]
  {:type :invoke, :f :update})

(defn checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [bad (filter (fn [op] (= :fail (:type op))) history)]
        {:valid? (not (seq bad))
         :errors bad}))))

(defn workload
  [opts]
  {:client (SimpleClient. nil)
   :generator (->> (gen/mix [read write])
                   (gen/time-limit (:time-limit opts)))
   :checker (checker)})
