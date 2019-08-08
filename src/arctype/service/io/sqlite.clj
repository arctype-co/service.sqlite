(ns arctype.service.io.sqlite
  (:require
    [cheshire.core :as json]
    [clojure.core.async :as async]
    [clojure.java.jdbc :as jdbc]
    [clojure.tools.logging :as log]
    [hikari-cp.core :as hikari-cp]
    [schema.core :as S]
    [sundbry.resource :as resource :refer [with-resources]]
    [arctype.service.protocol :refer :all])
  (:import
    [java.sql SQLException]))

(def Config
  {:db-spec {S/Keyword S/Any}})

(defn- xform-db-spec
  [spec]
  (cond-> spec
    (some? (:connection-uri spec)) (-> (assoc :jdbc-url (:connection-uri spec))
                                       (dissoc :connection-uri))
    ; Default to a single connection for SQLite
    (nil? (:maximum-pool-size spec)) (assoc :maximum-pool-size 1)))

(defn- connect
  "Open a jdbc connection pool"
  [config]
  (let [db-spec (xform-db-spec (:db-spec config))]
    (log/debug {:message "Opening Hikari connection pool to SQLite"
                :db-spec db-spec})
    (hikari-cp/make-datasource db-spec)))

(defn- disconnect
  "Release a jdbc connection pool"
  [db]
  (log/debug {:message "Closing Hikari connection pool to SQLite"})
  (hikari-cp/close-datasource db)
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

(defn first-val
  "Get the first column of the first row"
  [results]
  (second (ffirst results)))

(defrecord SQLiteClient [config datasource]
  PLifecycle
  (start [this]
    (log/info "Starting SQLite client")
    (-> this
        (assoc :datasource (connect config))))

  (stop [this]
    (log/info "Stopping SQLite client")
    (-> this
        (update :datasource disconnect)))

  PJdbcConnection
  (conn [this] 
    {:datasource datasource}))

(S/defn create
  [resource-name config :- Config]
  (resource/make-resource
    (map->SQLiteClient
      {:config config})
    resource-name))
