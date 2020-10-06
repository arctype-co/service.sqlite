(ns arctype.service.sqlite-test
  (:require
    [clojure.test :refer :all]
    [clojure.java.io :as io]
    [clojure.java.jdbc :as jdbc]
    [clojure.tools.logging :as log]
    [arctype.service.protocol :refer :all]
    [arctype.service.sqlite :as sqlite]
    [sundbry.resource :as resource]))

(def ^:dynamic *system* nil)
(def ^:dynamic *db* nil)

(def ^:private test-config
  {:db-spec
   {:connection-uri "jdbc:sqlite:test.db"
    :auto-commit? false}})

(defn- new-system
  []
  (let [db-file (io/file "test.db")]
    (when (.exists db-file)
      (.delete db-file)))
  (-> (resource/make-system
        {}
        :sqlite-test
        [(sqlite/create :db test-config)])
      (resource/initialize)
      (resource/invoke start)
      (start)))

(defn- destroy-system
  [system]
  (-> system
      (resource/invoke stop)))

(defn- with-system
  [test-fn]
  (binding [*system* (new-system)] 
    (binding [*db* (resource/require *system* :db)]
      (try
        (test-fn)
        (finally
          (destroy-system *system*))))))

(use-fixtures :each with-system)

(deftest test-basics
  (log/info "Test basic ops")
  (testing "Basic operations"
    (let [cxn (conn *db*)]
      (is (some? cxn))
      (jdbc/execute! cxn "create table generals (name varchar(255) primary key, state varchar(255), dob integer);")
      (is (empty? (jdbc/query cxn ["select * from generals"])))
      (jdbc/execute! cxn "insert into generals values ('George Washington', 'Virginia', 1732);")
      (is (= 1 (count (jdbc/query cxn ["select * from generals"]))))))
  (testing "Transactions"
    (let [cxn (conn *db*)]
      (jdbc/with-db-transaction [tx cxn]
        (jdbc/execute! tx "insert into generals values ('George Patton', 'California', 1885);") 
        (is (= 2 (count (jdbc/query cxn ["select * from generals"])))))
      (jdbc/with-db-transaction [tx cxn]
        (is (= 2 (count (jdbc/query cxn ["select * from generals"]))))))))

(defn- insert-event!
  [tx]
  (let [ts (System/nanoTime)
        data (str "{ timestamp: " ts " }")]
    (jdbc/execute! tx ["insert into events values(?, ?)" data ts])))

(defn event-thread-runner
  [db]
  (fn []
    (dotimes [n 1000]
      (sqlite/with-locking-tx [tx db]
        (insert-event! tx)))))

(deftest test-concurrency
  (log/info "Test concurrent ops")
  (let [cxn (conn *db*)]
    (is (some? cxn))
    (jdbc/execute! cxn "create table events (data text, timestamp integer);")
    (is (empty? (jdbc/query cxn ["select * from events"])))
    (insert-event! cxn)
    (is (= 1 (first (vals (first (jdbc/query cxn ["select count(*) from events"])))))))
  (let [threads (repeatedly 8 #(Thread. (event-thread-runner *db*)))
        cxn (conn *db*)]
    (doseq [thread threads]
      (.start thread))
    (doseq [thread threads]
      (.join thread))
    (jdbc/with-db-transaction [tx cxn]
      (is (= 8001 (first (vals (first (jdbc/query cxn ["select count(*) from events"])))))))))
