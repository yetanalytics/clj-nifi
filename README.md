# clj-nifi

A Clojure library/DSL for Apache NiFi processors.

## Usage

To use this in your project:

[![Clojars Project](https://img.shields.io/clojars/v/big-solutions/clj-nifi.svg)](https://clojars.org/big-solutions/clj-nifi)

API docs: [apidocs](http://htmlpreview.github.io/?https://github.com/big-solutions/clj-nifi/blob/master/doc/api/index.html)


## Yet Deployment

add your YAVEN_USERNAME and YAVEN_PASSWORD to your environment, then run the following:
```
$ boot build
$ boot push --repo yaven -f target/clj-nifi-0.1.1-SNAPSHOT.jar
```

## See also

- [boot-nifi](https://github.com/big-solutions/boot-nifi) - a Clojure DSL for Apache NiFi
- [clj-nifi-bundle] (https://github.com/big-solutions/clj-nifi) - Boot template for clj-nifi projects

## License

Copyright © 2016 Big Solutions

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
