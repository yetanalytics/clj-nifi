(ns clj-nifi.core-test
  (:require [clojure.test :refer :all]
            [clj-nifi.core :refer :all])
  (:import (org.apache.nifi.components PropertyDescriptor$Builder PropertyValue)
           (org.apache.nifi.processor.util StandardValidators FlowFileFilters)
           (org.apache.nifi.processor ProcessContext ProcessSession FlowFileFilter DataUnit)
           (org.apache.nifi.controller.queue QueueSize)
           (org.apache.nifi.flowfile FlowFile)))




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
          result (get-by {:session session} flt)])))
