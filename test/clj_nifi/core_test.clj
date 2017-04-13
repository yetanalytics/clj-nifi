(ns clj-nifi.core-test
  (:require [clojure.test :refer :all]
            [clj-nifi.core :refer :all]
            [clojure.java.io :only [output-stream]])
  (:import (org.apache.nifi.components PropertyDescriptor$Builder PropertyValue)
           (org.apache.nifi.processor.util StandardValidators FlowFileFilters)
           (org.apache.nifi.processor Relationship ProcessContext ProcessSession FlowFileFilter DataUnit)
           (org.apache.nifi.controller.queue QueueSize)
           (org.apache.nifi.controller ControllerService)
           (org.apache.nifi.flowfile FlowFile)
           (java.io ByteArrayOutputStream)
           (org.apache.nifi.processor.io OutputStreamCallback)
           (java.util.concurrent TimeUnit)))




(deftest relationship-test
  (testing "relationship object with default args"
    (let [relationship-obj (relationship :name "foo" :description "bar")]
     (is (-> relationship-obj .getName (= "foo")))
     (is (-> relationship-obj .getDescription (= "bar")))
     (is (-> relationship-obj .isAutoTerminated (= false))))
    )
  (testing "relationship object with auto-terminate"
    (let [relationship-obj (relationship :name "foo" :description "bar" :auto-terminate true)]
      (is (-> relationship-obj .isAutoTerminated (= true))))))


(deftest add-validators-test 
  (testing "add-validators adds validators to PropertyDescriptor$Builder"
    (let [descriptor (-> (new PropertyDescriptor$Builder)
                      (.name "test")
                      (add-validators [StandardValidators/NON_EMPTY_VALIDATOR])
                      (.build))
          result (-> descriptor .getValidators first)]
      (is (= result StandardValidators/NON_EMPTY_VALIDATOR)))))

(deftest property-test
  (testing "valid property object is set"
    (let [property-obj (property :name        "New name"
                                 :description "Name to rename the file"
                                 :validators  [StandardValidators/NON_EMPTY_VALIDATOR]
                                 :required?    true)]
      (is (-> property-obj .getName (= "New name")))
      (is (-> property-obj .getDescription (= "Name to rename the file")))
      (is (-> property-obj .getValidators first (= StandardValidators/NON_EMPTY_VALIDATOR)))
      (is (-> property-obj .isRequired true?))
      (is (-> property-obj .isDynamic false?))
      (is (-> property-obj .isExpressionLanguageSupported false?))
      (is (-> property-obj .getDefaultValue (= "")))
      (is (-> property-obj .getAllowableValues nil?)))))

(deftest queue-size-test ;; since the instance of session is something we only ever see at runtime, we use reify
  (testing "queue-size calls getQueueSize on instance of session"
    (let [state (atom :not-called)
          session (reify org.apache.nifi.processor.ProcessSession
                    (getQueueSize [session] 
                      (do (reset! state :called)
                       (org.apache.nifi.controller.queue.QueueSize. 1 1))))
          result (queue-size session)]
      (is (-> result (= (org.apache.nifi.controller.queue.QueueSize. 1 1))))
      (is (-> @state (= :called))))))


(deftest init-test
  (testing "init takes context and session and puts them in a map"
    (let [context (reify org.apache.nifi.processor.ProcessContext)
          session (reify org.apache.nifi.processor.ProcessSession)
          result (init context session)
          {c :context s :session} result]
      (is (-> c (= context)))
      (is (-> s (= session))))))

(deftest adjust-counter-test
  (testing "adjust counter calls .adjustCounter on the session object"
    (let [session (reify org.apache.nifi.processor.ProcessSession
                    (adjustCounter [session counter delta immediate?]
                      (is (-> counter (= "error counter")))
                      (is (-> delta (= 1)))
                      (is (-> immediate? true?))))]
      (adjust-counter {:session session} "error counter" 1 true))))

(deftest create-test
  (testing "create calls .create on the session object"
    (let [state (atom :ignored)
          flow-file (reify org.apache.nifi.flowfile.FlowFile)
          session (reify org.apache.nifi.processor.ProcessSession
                    (create [s]
                      (reset! state :called)
                      flow-file))
          result (create {:session session})
          {file :file} result
          ]
      (is (-> @state (= :called)))
      (is (-> file (= flow-file))))))

(deftest get-one-test
  (testing "get-one calls .get on the session object"
    (let [state (atom :ignored)
          flow-file (reify org.apache.nifi.flowfile.FlowFile)
          session (reify org.apache.nifi.processor.ProcessSession
                    (get [s]
                      (reset! state :called)
                      flow-file))
          result (get-one {:session session})
          {file :file} result]
      (is (-> @state (= :called)))
      (is (-> file (= flow-file))))))


;; not sure I'm happy with the multiplicative behavior of this function
(deftest get-batch-test
  (testing "when .get returns multiple files, maps files to the given map"
    (let [flow-files [:ff1 :ff2]
          session (reify org.apache.nifi.processor.ProcessSession
                    (^java.util.List get [s ^int mx]
                      flow-files))
         result (get-batch {:session session} 2)
         [_ last-map] result
         {:keys [file]} last-map]
      (is (-> result count (= 2))) 
      (is (-> file (= :ff2))))))

(deftest get-by-test
  (testing "when .get is called with a file filter, filters results as expected"
    (testing "when total file size is below the set filter limit all files come back"
     (let [flt (. FlowFileFilters (newSizeBasedFilter 20 org.apache.nifi.processor.DataUnit/B 200))
          flow-file (reify org.apache.nifi.flowfile.FlowFile
                      (^long getSize [_]
                        5))
          flow-files [flow-file flow-file flow-file]
          session (reify org.apache.nifi.processor.ProcessSession
                    (^java.util.List get [_ ^FlowFileFilter flt]
                      (reduce (fn [acc ff]
                                (let [filter-result (str (.filter flt ff))]
                                  (cond
                                    (= filter-result "ACCEPT_AND_CONTINUE") (conj acc ff)
                                    :else acc)
                                  )) [] flow-files)))
          resulting-files (get-by {:session session} flt)]
       (is (-> resulting-files count (= 3)))))))

(deftest penalize-test
  (testing "penalizes given flow-file as expected"
    (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
          session (reify org.apache.nifi.processor.ProcessSession
                    (penalize [_ ff]
                      ff
                      ))
          result (penalize {:session session :file flow-file})
          {penalized-file :file} result]
      (is (-> penalized-file (= flow-file))))))


(deftest remove-file-test
  (testing "removes given flow-file as expected"
    (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
          session (reify org.apache.nifi.processor.ProcessSession
                    (^void remove [_ ^org.apache.nifi.flowfile.FlowFile ff]
                      ff))
          result (remove-file {:session session :file flow-file})
          {removed-file :file} result]
      (is (-> removed-file (= flow-file))))))

(deftest clone-test
  (testing ".clone is given arguments as expected"
   (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
        offset 1
        size 1
        session (reify org.apache.nifi.processor.ProcessSession
                  (clone [_ ff]
                    (is (-> ff (= flow-file)))
                    ff)
                  (clone [_ ff o s]
                    (is (-> ff (= flow-file)))
                    (is (-> o (= offset)))
                    (is (-> s (= size)))
                    ff))]
    (clone {:session session} flow-file)
    (clone {:session session} flow-file offset size))))

(deftest put-attribute-test
  (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
        key "foo"
        value "bar"
        session (reify org.apache.nifi.processor.ProcessSession
                  (putAttribute [_ ff k v]
                    (is (-> ff (= flow-file)))
                    (is (-> k (= key)))
                    (is (-> v (= value)))
                    ff
                    ))]
    (put-attribute {:session session :file flow-file} key value)))


(deftest remove-attribute-test
  (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
        key "foo"
        session (reify org.apache.nifi.processor.ProcessSession
                  (removeAttribute [_ ff k]
                    (is (-> ff (= flow-file)))
                    (is (-> k (= key)))
                    ff
                    ))]
    (remove-attribute {:session session :file flow-file} key)))


(deftest remove-attributes-test
  (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
        ks #{"foo" "bar"}
        session (reify org.apache.nifi.processor.ProcessSession
                  (^org.apache.nifi.flowfile.FlowFile removeAllAttributes [^org.apache.nifi.processor.ProcessSession s 
                                                                           ^org.apache.nifi.flowfile.FlowFile ff 
                                                                           ^java.util.Set k]
                    (is (-> ff (= flow-file)))
                    (is (-> k (= ks)))
                    ff))]
    (remove-attributes {:session session :file flow-file} ks)))


(deftest remove-attributes-by-test
  (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
        re (re-pattern ".*") 
        session (reify org.apache.nifi.processor.ProcessSession
                  (^org.apache.nifi.flowfile.FlowFile removeAllAttributes [^org.apache.nifi.processor.ProcessSession s 
                                                                           ^org.apache.nifi.flowfile.FlowFile ff 
                                                                           ^java.util.regex.Pattern r]
                    (is (-> ff (= flow-file)))
                    (is (-> r (= re)))
                    ff))]
    (remove-attributes-by {:session session :file flow-file} re)))


(deftest output-callback-test
  (testing "when contents is a string..."
   (let [contents "foo"
        out (ByteArrayOutputStream.) 
        _ (-> (output-callback contents)
                (.process out))
        result (.toString out)]
    (is (-> result (= contents)))))
  (testing "when contents is a byte array..."
   (let [contents (.getBytes "foo")
        out (ByteArrayOutputStream.) 
        _ (-> (output-callback contents)
                (.process out))
        result (.toString out)]
    (is (-> result (= "foo"))))))


(deftest write-test
 (testing ".write method gets called on the session object"
  (let [contents "foo"
       out (ByteArrayOutputStream.)
       flow-file (reify org.apache.nifi.flowfile.FlowFile)
       session (reify org.apache.nifi.processor.ProcessSession
                 (^FlowFile write [_ ^FlowFile ff ^OutputStreamCallback cb]
                   (do (.process cb out) ;; need to process the callback to see what it holds
                    (is (-> flow-file (= ff))))
                    (is (-> out (.toString) (= "foo"))) 
                  ff))]
    (write {:file flow-file :session session} contents))))

(deftest append-test
 (testing ".append method gets called on the session object"
  (let [contents "foo"
       out (ByteArrayOutputStream.)
       flow-file (reify org.apache.nifi.flowfile.FlowFile)
       session (reify org.apache.nifi.processor.ProcessSession
                 (^FlowFile append [_ ^FlowFile ff ^OutputStreamCallback cb]
                   (do (.process cb out) ;; need to process the callback to see what it holds
                    (is (-> flow-file (= ff))))
                    (is (-> out (.toString) (= "foo"))) 
                  ff))]
    (append {:file flow-file :session session} contents))))

(deftest with-read-test)


(deftest import-from-test
  (testing ".importFrom method is called"
    (let [in (clojure.java.io/input-stream (.getBytes "foo"))
          flow-file (reify org.apache.nifi.flowfile.FlowFile)
          session (reify org.apache.nifi.processor.ProcessSession
                    (^FlowFile importFrom [_ ^java.io.InputStream i ^FlowFile file]
                      (is (-> file (= flow-file)))
                      (is (-> i (= in)))
                      file))]
      (import-from {:session session :file flow-file} in))))

(deftest export-to-test
  (testing ".exportTo method is called"
    (let [out (ByteArrayOutputStream.)
          flow-file (reify org.apache.nifi.flowfile.FlowFile)
          session (reify org.apache.nifi.processor.ProcessSession
                    (^void exportTo [_ ^FlowFile file ^java.io.OutputStream o]
                      (is (-> file (= flow-file)))
                      (is (-> o (= out)))
                      file))]
      (export-to {:session session :file flow-file} out))))

(deftest merge-files-test
  (testing ".merge method is called"
    (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
          flow-files [flow-file]
          session (reify org.apache.nifi.processor.ProcessSession
                   (merge [_ sources file]
                     (is (-> sources (= flow-files)))
                     (is (-> file (= flow-file)))
                     file))]
      (merge-files {:session session :file flow-file} flow-files))))

(deftest transfer-test
  (testing ".transfer is called"
    (let [flow-file (reify org.apache.nifi.flowfile.FlowFile)
          rel (relationship :name "success" :description "Success")
          session (reify org.apache.nifi.processor.ProcessSession
                    (^void transfer [_ ^FlowFile file ^Relationship r]
                      (is (-> file (= flow-file)))
                      (is (-> r (= rel)))))]
      (transfer {:session session :file flow-file} rel))))



(deftest as-string-test
  (let [property-value (reify org.apache.nifi.components.PropertyValue
                         (getValue [_]
                          "called"))
        result (as-string property-value)]
    (is (= result "called"))))

(deftest as-double-test
  (let [property-value (reify org.apache.nifi.components.PropertyValue
                         (asDouble [_]
                          0.0))
        result (as-double property-value)]
    (is (= result 0.0))))

(deftest as-float-test
  (let [property-value (reify org.apache.nifi.components.PropertyValue
                         (asFloat [_]
                          (float 0.0)))
        result (as-float property-value)]
    (is (= result 0.0))))

(deftest as-integer-test
  (let [property-value (reify org.apache.nifi.components.PropertyValue
                         (asInteger [_]
                          (int 0)))
        result (as-integer property-value)]
    (is (= result 0))))

(deftest as-long-test
  (let [property-value (reify org.apache.nifi.components.PropertyValue
                         (asLong [_]
                          (long 0)))
        result (as-long property-value)]
    (is (= result 0))))

(deftest as-data-size-test
  (let [property-value (reify org.apache.nifi.components.PropertyValue
                         (asDataSize [_ du]
                          0.0))
        result (as-data-size property-value DataUnit/B)]
    (is (= result 0.0))))


(deftest as-time-period-test
  (let [property-value (reify org.apache.nifi.components.PropertyValue
                         (asTimePeriod [_ du]
                          (long 0.0)))
        result (as-time-period property-value TimeUnit/NANOSECONDS)]
    (is (= result 0))))


(deftest as-controller-test) ; not clear on how to test this yet


(deftest get-property-test
  (let [property (reify org.apache.nifi.components.PropertyValue
                         (getValue [_]
                          "bar"))
        context (reify org.apache.nifi.processor.ProcessContext
                  (^PropertyValue getProperty [_ ^String prop]
                    (let [properties {"foo" property}
                          value (properties prop)]
                      value)))
        result (get-property context "foo")] 
    (is (= result property))))

(deftest get-properties-test
  (let [properties {"foo" "bar"}
        context (reify org.apache.nifi.processor.ProcessContext
                          (getProperties [_]
                            properties))
        result (get-properties context)]
    (is (= result properties))))


(deftest processor-name-test
  (let [nimi "foo" 
        context (reify org.apache.nifi.processor.ProcessContext
                          (getName [_]
                            nimi))
        result (processor-name context)]
    (is (= result nimi))))
