# About

github.com/jmoiron/sqlx CRUD wrapper:

 - Get - query reocrd
 - Gets - query records
 - Create - insert record 
 - Creates - insert records in bulk
 - CreateOrUpdate - create or update record
 - Update update record
 - Del - delete record

Search, MySQL *ONLY*

 - Search - query records with where EQUAL(=) and LIKE conditions
 - SearchFullText - query records with MySQL fulltext index

Misc

 - RawQuery - custom SQL
 - GetColumns - compose xx in `SELECT xx from ...`


For more detail about example, see `dbwrapper_test.go` .

Install 

    go get -v -u github.com/lib/pq
    go get -v -u github.com/go-sql-driver/mysql
    go get -v -u github.com/jmoiron/sqlx
    go get -v -u github.com/shuge/dbwrapper


## See also

- http://go-database-sql.org/index.html
- https://github.com/go-sql-driver/mysql
- https://github.com/jmoiron/sqlx
- https://github.com/lib/pq



