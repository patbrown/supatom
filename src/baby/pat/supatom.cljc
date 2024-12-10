(ns baby.pat.supatom
  (:require [baby.pat.jes.vt :as vt]
            [baby.pat.jes.vt.util :as u]
            [clojure.edn]
            [clojure.spec.alpha :as s]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]])
  #?(:bb (:import [clojure.lang IAtom IDeref IRef IMeta IObj Atom
                   IPersistentMap PersistentTreeMap
                   IPersistentSet PersistentTreeSet
                   PersistentQueue IPersistentVector]
                  [java.io Writer])
     :clj (:import clojure.lang.IAtom
                   clojure.lang.IDeref
                   clojure.lang.IRecord
                   java.io.Writer
                   java.util.concurrent.locks.ReentrantLock)))

(defprotocol IPersist
  (commit [this])
  (snapshot [this]))

(defmulti commit-with #(-> % :variant))
(defmulti snapshot-with #(-> % :variant))
(defmethod commit-with :default [this] (identity this))
(defmethod snapshot-with :default [this] (identity this))

#?(:clj (defmacro with-locking
          [lock & body]
          (if (s/valid? ::vt/reentrant-lock lock)
            `(try
               (.lock ~lock)
               ~@body
               (finally (.unlock ~lock)))
            (do `(locking ~lock
                   ~@body)))))

(defrecord Supatom [connection variant id commit-with snapshot-with read-with write-with lock source-atom watches-atom]
  #?@(:cljs [ISwap
             (-swap! [this f]
                     (let [original-value (read-with @source-atom)
                           new-value (write-with (f original-value))
                           result (reset! source-atom new-value)]
                       (commit this)
                       (read-with result)))
             (-swap! [this f a]
                     (let [original-value (read-with @source-atom)
                           new-value (write-with (f original-value a))
                           result (reset! source-atom new-value)]
                       (commit this)
                       (read-with result)))
             (-swap! [this f a b]
                     (let [original-value (read-with @source-atom)
                           new-value (write-with (f original-value a b))
                           result (reset! source-atom new-value)]
                       (commit this)
                       (read-with result)))
             IReset
             (-reset! [this new-value]
                      (let [result (reset! source-atom (write-with new-value))]
                        (commit this)
                        (read-with result)))
             IDeref
             (-deref [_] (read-with (deref source-atom)))
             IWatchable
             (-notify-watches [this oldval newval]
                              (doseq [[k f] (let [watch-instances (-> @watches-atom vals vals)]
                                              (map (fn [w] [(:supatom-watcher/id w)
                                                            (:supatom-watcher/supatom-watcher w)])
                                                   watch-instances))]
                                (f k this oldval newval)))
             (-add-watch [this k f]
                         (swap! watches-atom assoc k f)
                         (add-watch source-atom k
                                    (fn [_ _ old-value new-value]
                                      (when-not (= old-value new-value)
                                        (f k this old-value new-value))))
                         this)
             (-remove-watch [this k]
                            (swap! watches-atom dissoc k)
                            (remove-watch source-atom k) this)
             IPersist
             (commit [this] (commit-with this))
             (snapshot [this] (snapshot-with this))]

      :bb [clojure.lang.IAtom
           (swap [this f]
                 (with-locking lock
                   (let [original-value (read-with @source-atom)
                         new-value (write-with (f original-value))
                         result (read-with (reset! source-atom new-value))
                         _ (commit this)]
                     result)))
           (swap [this f a]
                 (with-locking lock
                   (let [original-value (read-with @source-atom)
                         new-value (write-with (f original-value a))
                         result (read-with (reset! source-atom new-value))
                         _ (commit this)]
                     result)))
           (swap [this f a b]
                 (with-locking lock
                   (let [original-value (read-with @source-atom)
                         new-value (write-with (f original-value a b))
                         result (read-with (reset! source-atom new-value))
                         _ (commit this)]
                     result)))
           (swap [this f a b c]
                 (with-locking lock
                   (let [original-value (read-with @source-atom)
                         new-value (write-with (f original-value a b c))
                         result (read-with (reset! source-atom new-value))
                         _ (commit this)]
                     result)))
           (reset [this new-value]
                  (with-locking lock
                    (let [newer-value (write-with new-value)
                          result (read-with (reset! source-atom newer-value))
                          _ (commit this)]
                      result)))
           clojure.lang.IDeref
           (deref [_] (read-with (deref source-atom)))
           IPersist
           (commit [this] (commit-with this))
           (snapshot [this] (snapshot-with this))]

      :clj [clojure.lang.IAtom
            (swap
             [this f]
             (with-locking lock
               (let [original-value (read-with @source-atom)
                     new-value (write-with (f original-value))
                     result (reset! source-atom new-value)]
                 (commit this)
                 (read-with result))))
            (swap [this f a]
                  (with-locking lock
                    (let [original-value (read-with @source-atom)
                          new-value (write-with (f original-value a))
                          result (reset! source-atom new-value)]
                      (commit this)
                      (read-with result))))
            (swap [this f a b]
                  (with-locking lock
                    (let [original-value (read-with @source-atom)
                          new-value (write-with (f original-value a b))
                          result (reset! source-atom new-value)]
                      (commit this)
                      (read-with result))))
            (swap [this f a b c]
                  (with-locking lock
                    (let [original-value (read-with @source-atom)
                          new-value (write-with (f original-value a b c))
                          result (reset! source-atom new-value)]
                      (commit this)
                      (read-with result))))
            (reset [this new-value]
                   (with-locking lock
                     (let [newer-value (write-with new-value)
                           result (reset! source-atom newer-value)]
                       (commit this)
                       (read-with result))))
            clojure.lang.IDeref
            (deref [_] (read-with (deref source-atom)))
            clojure.lang.IRef
            (getWatches [this]
                        @watches-atom)
            (addWatch [this k f]
                      (swap! watches-atom assoc k f)
                      (add-watch source-atom k
                                 (fn [_ _ old-value new-value]
                                   (when-not (= old-value new-value)
                                     (f k this old-value new-value))))
                      this)
            (removeWatch [this k]
                         (swap! watches-atom dissoc k)
                         (remove-watch source-atom k) this)
            IPersist
            (commit [this] (commit-with this))
            (snapshot [this] (snapshot-with this))]))

(deftype Supalink [source-atom meta validate-with watches-atom get-with set-with id]

  #?@(:bb [Object
           (equals [_ other]
                   (and (instance? Supalink other)
                        (= source-atom (.-source-atom other))))
           clojure.lang.IAtom
           (swap [this f]
                 (reset! this (f (deref source-atom))))
           (swap [this f x]
                 (reset! this (f (deref source-atom) x)))
           (swap [this f x y]
                 (reset! this (f (deref source-atom) x y)))
           (swap [this f x y z]
                 (reset! this (apply f (deref source-atom) x y z)))
           (compareAndSet [_ old-value new-value]
                          (let [_ (compare-and-set! source-atom old-value new-value)
                                commit? (= old-value new-value)]
                            (when commit?
                              (swap! source-atom new-value))))
           (reset [this new-value]
                  (if set-with
                    (let [validate (.-validate-with this)]
                      (when-not (nil? validate)
                        (assert (validate new-value) "Invalid Value"))
                      (swap! source-atom set-with new-value)
                      new-value)
                    (throw (Exception. "READ ONLY ATOM: Reset unavailable."))))
           clojure.lang.IDeref
           (deref [this]
                  (get-with @source-atom))]

      :clj [Object
            (equals [_ other]
                    (and (instance? Supalink other)
                         (= source-atom (.-source-atom other))))
            clojure.lang.IAtom
            (swap [this f]
                  (reset! this (f (deref source-atom))))
            (swap [this f x]
                  (reset! this (f (deref source-atom) x)))
            (swap [this f x y]
                  (reset! this (f (deref source-atom) x y)))
            (swap [this f x y z]
                  (reset! this (apply f (deref source-atom) x y z)))
            (compareAndSet [_ old-value new-value]
                           (let [_ (compare-and-set! source-atom old-value new-value)
                                 commit? (= old-value new-value)]
                             (when commit?
                               (swap! source-atom new-value))))
            (reset [this new-value]
                   (if set-with
                     (let [validate (.-validate-with this)]
                       (when-not (nil? validate)
                         (assert (validate new-value) "Invalid Value"))
                       (swap! source-atom set-with new-value)
                       new-value)
                     (throw (Exception. "READ ONLY ATOM: Reset unavailable."))))
            clojure.lang.IDeref
            (deref [this]
                   (get-with (deref source-atom)))
            clojure.lang.IRef
            (getWatches [this]
                        @watches-atom)
            (addWatch [this k f]
                      (swap! watches-atom assoc k f)
                      (add-watch source-atom k
                                 (fn [_ _ old-value new-value]
                                   (when-not (= old-value new-value)
                                     (f key this old-value new-value))))
                      this)
            (removeWatch [this k]
                         (swap! watches-atom dissoc k)
                         (remove-watch source-atom k)
                         this)]

      :cljs [Object
             (equiv [this other] (-equiv this other))
             IDeref
             (-deref [this]
                     (get-with (deref source-atom)))
             IMeta
             (-meta [_] meta)
             IWithMeta
             (-with-meta [o meta]
                         (Supalink. source-atom meta validate-with watches-atom get-with set-with id))
             IWatchable
             (-notify-watches [this oldval newval]
                              (doseq [[k f] (let [watch-instances (-> @watches-atom vals vals)]
                                              (map (fn [{:supalink/keys [id supalink-watcher]}]
                                                     [id supalink-watcher])
                                                   watch-instances))]
                                (f k this oldval newval)))
             (-add-watch [this k f]
                         (swap! watches-atom assoc k f)
                         (add-watch source-atom k
                                    (fn [_ _ old-value new-value]
                                      (when-not (= old-value new-value)
                                        (f k this old-value new-value))))
                         this)
             IReset
             (-reset! [this new-value]
                      (if set-with
                        (let [validate (.-validate-with this)]
                          (when-not (nil? validate)
                            (assert (validate new-value) "Invalid Value"))
                          (swap! source-atom set-with new-value)
                          new-value)
                        (throw (js/Exception. "READ ONLY ATOM: Reset unavailable."))))
             ISwap
             (-swap! [this f] (reset! this (f (deref source-atom))))
             (-swap! [this f x] (reset! this (f (deref source-atom) x)))
             (-swap! [this f x y] (reset! this (f (deref source-atom) x y)))]))

;; Just kept it dumb AF.   
#?(:clj (defmethod print-method Supatom [x w]
          ((get-method print-method clojure.lang.IRecord) x w)))

#?(:clj (defmethod print-method Supalink [x w]
          ((get-method print-method clojure.lang.IRecord) x w)))

(defn-spec supatom? ::vt/?
  "Is a thing a supatom?"
  [thing ::vt/any]
  (instance? Supatom thing))

(s/def ::vt/supatom #(supatom? %))

#?(:clj (defmethod print-method Supalink [supa w]
          (.write w "#")
          (.write w (-> supa class .getName))
          (.write w (format " 0x%x " (System/identityHashCode supa)))
          (.write w " {:status :ready, :val ")
          (.write w (-> (.-source_atom supa) deref pr-str))
          (.write w "}")))

(def supatom-default-base {:variant :mem/default
                           :backing :mem
                           :commit-with commit-with
                           :snapshot-with snapshot-with
                           :read-with clojure.edn/read-string
                           :write-with str})

(defn fresh-lock [] #?(:bb (Object.)
                       :clj (ReentrantLock.)
                       :cljs (Object.)))

(defn-spec *supatom ::vt/fn
  "Takes a config map and returns a function that takes another config map and makes a supatom."
  ([overlay-config ::vt/map]
   (fn [config]
     (let [{:keys [backing write-with source-atom] :as new-config}
           (merge supatom-default-base
                  {:source-atom (atom nil)
                   :lock (fresh-lock)
                   :watches-atom (atom {})}
                  overlay-config
                  config)
           _ (when-let [snap (snapshot-with new-config)]
               (reset! source-atom (write-with snap)))
           supatom (map->Supatom new-config)]
       (if (= backing :mem)
         (let [_ (reset! supatom {})]
           supatom)
         supatom)))))

(defn-spec supatom-> ::vt/supatom
  "Simplest supatom creation function."
  ([] (*supatom {:id (u/qid)}))
  ([overlay-config ::vt/map] ((*supatom overlay-config) {})))

(defn-spec supalink? ::vt/?
  "Is a thing a supalink?"
  [thing ::vt/any]
  (instance? Supalink thing))

(s/def ::vt/supalink #(supalink? %))

(defn-spec map->Supalink ::vt/supalink
  "Takes a map and creates a supalink."
  [{:keys [source-atom get-with set-with meta validate-with watches-atom id]} ::vt/map]
  (Supalink. source-atom meta validate-with watches-atom get-with set-with id))

(def supalink-defaults
  {:get-with (fn [x] x)
   :set-with (fn [a link-value] (merge a link-value))
   :validate-with any?
   :meta {:supalink? true}
   :source-atom (atom {})
   :watches-atom (atom {})})

(defn-spec supalink ::vt/supalink
  "Simplest supalink creation function"
  ([config ::vt/map]
   (map->Supalink (merge supalink-defaults config)))
  ([source-atom ::vt/atom get-with ::vt/fn]
   (supalink {:source-atom source-atom :get-with get-with}))
  ([source-atom ::vt/atom get-with ::vt/fn set-with ::vt/fn]
   (supalink {:source-atom source-atom :get-with get-with :set-with set-with})))

(def link-> "Alias to create simple supalink functions." supalink)

(defn-spec cursor-> #(supalink? %)
  "Creates a cursor from the supplied atom using a supplied path."
  ([{:keys [source-atom path]} ::vt/map] (cursor-> source-atom path))
  ([source-atom ::vt/atom path ::vt/vec]
   (if-not (seq path)
     source-atom
     (link-> source-atom
             #(get-in % path)
             #(assoc-in %1 path %2)))))

(defn-spec xform-> #(supalink? %)
  "Xfroms a source atom to the destination atom using the supplied fn."
  ([{:keys [source-atom xform destination-atom]} ::vt/map] (xform-> source-atom xform destination-atom))
  ([source-atom ::vt/atom xform ::vt/fn dest ::vt/atom]
   (link-> source-atom
           (fn [x] (reset! (or dest source-atom) (xform x)))
           (fn [x] (xform x)))))

(defn-spec count-> #(supalink? %)
  "Returns the count in the atom along the supplied path."
  ([{:keys [source-atom path]} ::vt/map]
   (count-> source-atom path))
  ([source-atom ::vt/atom path ::vt/vec]
   (link-> source-atom #(let [v (get-in % (or path []))]
                          (count v)))))

(defn-spec head-> #(supalink? %)
  "Returns the first `quantity` entries from the atom."
  ([{:keys [source-atom path quantity]} ::vt/map]
   (head-> source-atom path quantity))
  ([source-atom ::vt/atom path ::vt/vec quantity ::vt/long]
   (link-> source-atom #(take quantity (reverse (get-in % (or path [])))))))

(defn-spec tail-> #(supalink? %)
  "Returns the last `quantity` entries from the atom."
  ([{:keys [source-atom path quantity]} ::vt/map]
   (tail-> source-atom path quantity))
  ([source-atom ::vt/atom path ::vt/vec quantity ::vt/long]
   (link-> source-atom #(take quantity (get-in % (or path []))))))

(defn-spec backup-> #(supalink? %)
  "Create a backup atom."
  ([{:keys [source-atom destination-atom]} ::vt/map]
   (backup-> source-atom destination-atom))
  ([source-atom ::vt/atom dest ::vt/atom]
   (link-> source-atom #(let [v (get-in % [])]
                          (reset! dest v)))))
