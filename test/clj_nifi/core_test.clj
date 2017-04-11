(ns clj-nifi.core-test
  (:require [clojure.test :refer :all]
            [clj-nifi.core :refer :all])
  (:import (org.apache.nifi.components PropertyDescriptor$Builder PropertyValue)
           (org.apache.nifi.processor.util StandardValidators)
           (org.apache.nifi.processor ProcessContext ProcessSession)
           (org.apache.nifi.controller.queue QueueSize)
           ))

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

(deftest queue-size-test ;; since session is not something thats easy to mock, lets use with-redefs
  (testing "returns the queue size of a session"
    (let [session (reify org.apache.nifi.processor.ProcessSession
                    (getQueueSize [session] 
                      (do (println "foo!")
                       (org.apache.nifi.controller.queue.QueueSize. 1 1))))
          result (queue-size session)
          
          ]
      (is (-> result (= (org.apache.nifi.controller.queue.QueueSize. 1 1)))))))

