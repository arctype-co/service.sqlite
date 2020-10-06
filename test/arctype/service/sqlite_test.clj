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
   {:connection-uri "jdbc:sqlite:test.db"}})

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

(deftest test-basic-ops
  (log/info "Test basic ops")
  (let [cxn (conn *db*)]
    (is (some? cxn))
    (jdbc/execute! cxn "create table generals (name varchar(255) primary key, dob integer);")
    (is (empty? (jdbc/query cxn ["select * from generals"])))
    (jdbc/execute! cxn "insert into generals values ('George Washington', 1732);")
    (is (= 1 (count (jdbc/query cxn ["select * from generals"]))))))
