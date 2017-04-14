(def project 'com.yetanalytics/clj-nifi)
(def version "0.1.2-SNAPSHOT")
(def description "Clojure DSL for Apache NiFi")

(set-env! :resource-paths #{"resources" "src" "test"}
          :dependencies   '[[org.clojure/clojure "1.8.0"]
                            [org.apache.nifi/nifi-api "1.1.2"]
                            [org.apache.nifi/nifi-processor-utils "1.1.2"]
                            [funcool/boot-codeina "0.1.0-SNAPSHOT" :scope "test"]
                            [adzerk/boot-test "1.2.0" :scope "test"]]
          :repositories #(conj % ["yaven" {:url "https://yaven.yetanalytics.io/content/repositories/snapshots"
                                           :username (System/getenv "YAVEN_USERNAME") 
                                           :password (System/getenv "YAVEN_PASSWORD")}]))

(require '[funcool.boot-codeina :refer [apidoc]])
(require '[adzerk.boot-test :refer :all])

(task-options!
 pom {:project     project
      :version     version
      :description description
      :url         "https://github.com/big-solutions/clj-nifi"
      :scm         {:url "https://github.com/big-solutions/clj-nifi"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}}
 apidoc {:version     version
         :title       (name project)
         :sources     #{"src"}
         :description description})

(deftask build
  "Build and install the project locally."
  []
  (comp (pom) (apidoc) (aot :all true) (jar) (install) (target)))
