# Enhanced Korma

Modified version of Chris Granger's korma library containing some important enhancements

## Getting started

Simply add Korma as a dependency to your lein/cake project:

```clojure
[korma-enhanced "0.3.0"]
```

## Usage

For most of the docs and real usage, check out original korma documentation http://sqlkorma.com

## Differences of enhanced version

Enhanced korma differs in some aspects, here i will list all of them

* By default, only fields listed in entity definition are fetched in select queries
* There are 4 kinds of relations, has-one, has-many, has-many-to-many and belongs-to-many-to-many
* Unlike original korma, has-one relation doesn't return superset of both entity fields, but works with subquery as has-many
* All relations are supported also in add, update and delete queries

### Examples

Here we will create one user and assign him roles with ids 1 and 2, then we will update the same user and assign him roles 1,2 and 3 and last, we will delete the user also with all relations (in this case entries in many-to-many table).

```clojure
(declare roles)
(korma/defentity users               
  (korma/entity-fields :firstname :lastname :username :password)
  (korma/has-many-to-many roles))
  
(korma/defentity roles
  (korma/entity-fields :rolename)
  (korma/belongs-to-many-to-many users))
  
(korma/insert users
  (korma/values {:firstname "Jan" :lastname "Herich" :username "herichj" :id 1})
  (korma/relations {:roles [1 2]}))  
  
(korma/update users
  (korma/where {:id 1})
  (korma/relations {:roles [1 2 3]}))
  
(korma/delete users
  (korma/where {:id 1})
  (korma/add-deletion-of-relations))
```

## License

Copyright (C) 2012 Jan Herich

Distributed under the Eclipse Public License, the same as Clojure.
