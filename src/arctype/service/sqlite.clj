(ns arctype.service.sqlite
  (:require
    [clojure.core.async :as async]
    [clojure.java.jdbc :as jdbc]
    [clojure.tools.logging :as log]
    [schema.core :as S]
    [sundbry.resource :as resource :refer [with-resources]]
    [arctype.service.protocol :refer :all])
  (:import
    [java.sql SQLException]))

(def Config
  {:db-spec {S/Keyword S/Any}})

(defn- connect
  "Open a jdbc connection pool"
  [config]
  (let [db-spec (:db-spec config)]
    (log/debug {:message "Opening connection to SQLite"
                :db-spec db-spec})
    (jdbc/get-connection db-spec)))

(defn- disconnect
  "Release a jdbc connection pool"
  [db]
  (log/debug {:message "Closing connection to SQLite"})
  (.close db)
  nil)

(defn health-check!
  [this]
  (jdbc/query (conn this) ["SELECT version()"]))

(defmacro try-sql
  [& body]
  `(try 
     (do ~@body)
     (catch SQLException e#
       (let [inner# (.getNextException e#)]
         (log/error e# {:message "SQL exception"
                        :cause (when (some? inner#)
                                 (.getMessage inner#))}))
       (throw e#))))

(defmacro with-locking-tx
  [[tx db] & body]
  (let [fn-bindings [tx]]
  `(locking ~db
     (try-sql 
       (jdbc/db-transaction* ~db
                    (^{:once true} fn* ~fn-bindings ~@body))))))

(defn first-val
  "Get the first column of the first row"
  [results]
  (second (ffirst results)))

(defrecord SQLiteClient [config connection]
  PLifecycle
  (start [this]
    (log/info "Starting SQLite client")
    (-> this
        (assoc :connection (connect config))))

  (stop [this]
    (log/info "Stopping SQLite client")
    (-> this
        (update :connection disconnect)))

  PJdbcConnection
  (conn [this] 
    {:connection connection}))

(S/defn create
  [resource-name config :- Config]
  (resource/make-resource
    (map->SQLiteClient
      {:config config})
    resource-name))
