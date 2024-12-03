(ns baby.pat.supatom.redis
  (:require [orchestra.core :refer [defn-spec]]
            [baby.pat.secrets :as !]
            [baby.pat.supatom]
            [baby.pat.vt :as vt]
            [taoensso.carmine :as redis]
            [taoensso.nippy :refer [freeze thaw]]))

;; ## Outline
;; - map of 'redis-key' known prefixes (prepended strings)
;; - commit! function, dumb
;; - snapshot function, dumb
;; - defmethod baby.pat.supatom/commit-with
;; - defmethod baby.pat.supatom/snapshot-with 
;; - supatom-redis-default-overlay
;; - supatom - fn to create redis supatoms
;; Helpers

(defn connect-with [variant]
  {:pool (redis/connection-pool {})
   :spec {:uri (!/get-secret (keyword (str (namespace variant) "." (name variant)) "connection-string"))
          :ssl-fn :default}})

(defn *get [config k]
  (redis/wcar config (redis/get k)))

(defn *set [config k v]
  (redis/wcar config (redis/set k v)))

(defn *delete [config k]
  (redis/wcar config (redis/del k)))

(defn *keys
  ([config] (*keys config "*"))
  ([config k]
   (redis/wcar config (redis/keys k))))

(defn *exists? [config k]
  (= 1 (redis/wcar config (redis/exists k))))

(def supatom-variants-raw
  (mapv (fn [variant]
          {:supatom-variant/id variant
           :supatom-variant/backing :redis
           :supatom-variant/connection (connect-with variant)})
        [:redis/default
         :cache/build
         :cache/dev
         :cache/main
         :cache/shadow
         :cache/test]))

(def supatom-variants (vt/add :default {} supatom-variants-raw))

;; ### COMMIT!
(defn-spec commit! ::vt/discard
  "Commits contents to a redis key."
  ([id ::vt/qkw contents ::vt/any] (commit! supatom-variants :redis/default id contents))
  ([variant ::vt/qkw id ::vt/qkw contents ::vt/any] (commit! supatom-variants variant id contents))
  ([universe ::vt/map variant ::vt/qkw id ::vt/qkw contents ::vt/any]
   (let [config (vt/<- universe [:supatom-variant/id variant :supatom-variant/connection])]
     (*set config id contents))))

;; ### SNAPSHOT
(defn-spec snapshot ::vt/discard
  "Returns a snapshot from a file."
  ([id ::vt/qkw] (snapshot supatom-variants :redis/default id))
  ([variant ::vt/qkw id ::vt/qkw] (snapshot supatom-variants variant id))
  ([universe ::vt/map variant ::vt/qkw id ::vt/qkw]
   (let [config (vt/<- universe [:supatom-variant/id variant :supatom-variant/connection])]
     (if (*exists? config id)
       (*get config id)
       (commit! variant config id nil)))))

(defmethod baby.pat.supatom/commit-with :redis/default [{:keys [id write-with] :as this}]
  (commit! :redis/default id (write-with @this)))

(defmethod baby.pat.supatom/snapshot-with :redis/default [{:keys [id read-with] :as this}]
  (read-with (snapshot :redis/default id)))

(def supatom-redis-default-overlay {:variant :redis/default
                                    :write-with freeze
                                    :read-with thaw})

(defn supatom [config]
  (let [config (merge supatom-redis-default-overlay config)]
    (baby.pat.supatom/supatom-> config)))

(defn-spec checkout ::vt/supatom [stage ::vt/kw-or-str dt ::vt/kw-or-str]
  (supatom {:id (keyword (vt/singular dt) "id")
            :variant :dt/directory
            :connection {:variant (keyword "local" (name stage))}}))
