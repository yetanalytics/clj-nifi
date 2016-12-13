(ns clj-nifi.core
  (:import (org.apache.nifi.components PropertyDescriptor$Builder)
           (org.apache.nifi.processor Relationship$Builder ProcessSession)
           (org.apache.nifi.flowfile FlowFile)
           (org.apache.nifi.processor.io OutputStreamCallback)))

(defn relationship [& {:keys [name description auto-terminate]
                       :or {name           "Relationship"
                            description    "Description"
                            auto-terminate false}}]
  (-> (new Relationship$Builder)
      (.name name)
      (.description description)
      (.autoTerminateDefault auto-terminate)
      (.build)))

(defn add-validators [builder validators]
  (reduce #(.addValidator ^PropertyDescriptor$Builder % %2)
          builder validators))

(defn property [&{:keys [name display-name description required? dynamic? sensitive?
                         expressions? default validators allowable-values]
                  :or {name             "Property"
                       display-name     "Property"
                       description      "Description"
                       required?        false
                       dynamic?         false
                       sensitive?       false
                       expressions?     false
                       default          ""
                       validators       []
                       allowable-values nil}}]
  (-> (new PropertyDescriptor$Builder)
      (.name name)
      (.description description)
      (.required required?)
      (.dynamic dynamic?)
      (.sensitive sensitive?)
      (.expressionLanguageSupported expressions?)
      (.defaultValue default)
      (add-validators validators)
      (.allowableValues (if allowable-values (set allowable-values)))
      (.build)))

(defn init [context session]
  {:context context :session session})

(defn create [{:keys [session]}]
  {:session session
   :file (.create session)})

(defn get-one [{:keys [session]}]
  {:session session
   :file (.get session)})

(defn penalize [{:keys [session file]}]
  {:session session
   :file (.penalize session file)})

(defn put-attribute [{:keys [session file]} k v]
  {:session session
   :file (.putAttribute session file k v)})

(defn remove-attribute [{:keys [session file]} k]
  {:session session
   :file (.removeAttribute session file k)})

(defn output-callback [contents]
  (reify OutputStreamCallback
    (process [_ out]
      (spit out contents))))

(defn demo [session]
  (-> (init nil session)
      create
      (put-attribute "a" "123")
      (put-attribute "b" "abc")
      (remove-attribute "a")))