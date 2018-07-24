// github.com/jmoiron/sqlx CRUD wrapper. DO NOT REPEAT YOURSELF.
package dbwrapper

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/jmoiron/sqlx"
)

var (
	ErrRecordNotFound      = errors.New("record not found")
	ErrDuplicatedUniqueKey = errors.New("duplicated unique key")
)

type DBWrapper struct {
	Dsn       string
	Debug     bool
	TableName string
}

// NewDBWrapper setup DSN(data source name) and table, sub-class have to override this.
func NewDBWrapper() *DBWrapper {
	w := new(DBWrapper)
	w.Dsn = "test:test@tcp(127.0.0.1:3306)/test?charset=utf8mb4,utf8&timeout=2s&writeTimeout=2s&readTimeout=2s&parseTime=true"
	w.TableName = "test"
	return w
}

func (this *DBWrapper) OpenDB() (db *sqlx.DB, err error) {
	db, err = sqlx.Open("mysql", this.Dsn)
	if err != nil {
		return
	}

	err = db.Ping()
	return
}

func (this *DBWrapper) MustOpenDB() (db *sqlx.DB) {
	db, err := sqlx.Open("mysql", this.Dsn)
	if err != nil {
		log.Fatalln(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalln(err)
	}
	return
}

// Get returns one record at most.
// parameter `obj`` must be pass by `&MyObject{}`.`
func (this *DBWrapper) Get(db *sqlx.DB, obj interface{}, fields []string, pkName string, pk interface{}) (err error) {
	if db == nil {
		db, err = this.OpenDB()
		if err != nil {
			return
		}
		defer db.Close()
	}

	var queryFields string
	if len(fields) > 0 {
		queryFields = strings.Join(fields, ",")
	} else {
		queryFields = "*"
	}
	s := fmt.Sprintf("SELECT %s FROM %s WHERE %s=? LIMIT 1", queryFields, this.TableName, pkName)
	if this.Debug {
		log.Println("Sql", s)
		log.Println(" Parameters", pk)
	}
	err = db.Get(obj, s, pk)
	if err == sql.ErrNoRows {
		err = ErrRecordNotFound
	}
	return err

}

func (this *DBWrapper) Gets(
	db *sqlx.DB, objs interface{},
	fields []string,
	conditionsWhere *map[string]interface{},
	limit int) (err error) {
	if db == nil {
		db, err = this.OpenDB()
		if err != nil {
			return
		}
		defer db.Close()
	}

	var queryFields string
	if len(fields) > 0 {
		queryFields = strings.Join(fields, ",")
	} else {
		queryFields = "*"
	}

	var k string
	wheres := []string{}
	args := []interface{}{}
	for k = range *conditionsWhere {
		wheres = append(wheres, fmt.Sprintf("%s= ? ", k))
		value := (*conditionsWhere)[k]
		args = append(args, value)
	}

	var s string
	if len(wheres) > 0 {
		s = fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT %d",
			queryFields,
			this.TableName,
			strings.Join(wheres, " AND "),
			limit)
	} else {
		s = fmt.Sprintf("SELECT %s FROM %s LIMIT %d",
			queryFields,
			this.TableName,
			limit)

	}

	if this.Debug {
		log.Println("Sql", s)
		log.Println(" Parameters", args)
	}

	err = db.Select(objs, s, args...)
	return
}

func (this *DBWrapper) Search(
	db *sqlx.DB, objs interface{},
	fields []string,
	conditionsWhere *map[string]interface{},
	conditionsLike *map[string]interface{},
	limit int) (err error) {
	if db == nil {
		db, err = this.OpenDB()
		if err != nil {
			return
		}
		defer db.Close()
	}

	var queryFields string
	if len(fields) > 0 {
		queryFields = strings.Join(fields, ",")
	} else {
		queryFields = "*"
	}

	var k string
	wheres := []string{}
	// args := map[string]interface{}{}
	args := []interface{}{}
	if conditionsWhere != nil {
		for k = range *conditionsWhere {
			wheres = append(wheres, fmt.Sprintf("%s= ? ", k))
			value := (*conditionsWhere)[k]
			args = append(args, value)
		}
	}

	log.Println("conditionsWhere", conditionsWhere)
	log.Println("conditionsLike", conditionsLike)

	if conditionsLike != nil {
		for k = range *conditionsLike {
			wheres = append(wheres, fmt.Sprintf("%s LIKE ?", k))
			value := fmt.Sprintf(`%%%s%%`, (*conditionsLike)[k])
			args = append(args, value)
		}
	}

	s := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT %d",
		queryFields,
		this.TableName,
		strings.Join(wheres, " AND "),
		limit)

	if this.Debug {
		log.Println("Sql", s)
		log.Println(" Parameters", args)
	}

	err = db.Select(objs, s, args...)
	return
}

func (this *DBWrapper) CreateOrUpdate(db *sqlx.DB, m *map[string]interface{}) (result sql.Result, err error) {
	if db == nil {
		db, err = this.OpenDB()
		if err != nil {
			return
		}
		defer db.Close()
	}

	createKeys := []string{}
	createValuesPlaceholder := []string{}
	updates := []string{}

	for k := range *m {
		createKeys = append(createKeys, k)
		createValuesPlaceholder = append(createValuesPlaceholder, fmt.Sprintf(":%s", k))
		updates = append(updates, fmt.Sprintf("%s=:%s", k, k))

	}

	s := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		this.TableName,
		strings.Join(createKeys, ","),
		strings.Join(createValuesPlaceholder, ","),
		strings.Join(updates, ","),
	)
	if this.Debug {
		log.Println("Sql", s)
		log.Println(" Parameters", m)
	}
	result, err = db.NamedExec(s, *m)
	if err != nil {
		if mysqlError, ok := err.(*mysql.MySQLError); ok {
			if mysqlError.Number == 1062 {
				err = ErrDuplicatedUniqueKey
			}
		}
	}

	return
}

func (this *DBWrapper) Update(
	db *sqlx.DB,
	pkName string,
	changes map[string]interface{},
	where []string,
) (result sql.Result, err error) {
	if db == nil {
		db, err = this.OpenDB()
		if err != nil {
			return
		}
		defer db.Close()
	}

	updates := []string{}

	for k := range changes {
		if k == pkName {
			continue
		}
		updates = append(updates, fmt.Sprintf("%s=:%s", k, k))

	}

	s := fmt.Sprintf("UPDATE %s SET %s WHERE %s=:%s LIMIT 1",
		this.TableName,
		strings.Join(updates, ","),
		pkName,
		pkName,
	)
	if this.Debug {
		log.Println("Sql", s)
		log.Println(" Parameters", changes)
	}
	result, err = db.NamedExec(s, changes)
	if err != nil {
		if mysqlError, ok := err.(*mysql.MySQLError); ok {
			if mysqlError.Number == 1062 {
				err = ErrDuplicatedUniqueKey
			}
		}
	}
	return
}

func (this *DBWrapper) Create(db *sqlx.DB, m *map[string]interface{}) (result sql.Result, err error) {
	if db == nil {
		db, err = this.OpenDB()
		if err != nil {
			return
		}
		defer db.Close()
	}

	createKeys := []string{}
	createValuesPlaceholder := []string{}
	updates := []string{}

	for k := range *m {
		createKeys = append(createKeys, k)
		createValuesPlaceholder = append(createValuesPlaceholder, fmt.Sprintf(":%s", k))
		updates = append(updates, fmt.Sprintf("%s=:%s", k, k))

	}

	s := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		this.TableName,
		strings.Join(createKeys, ","),
		strings.Join(createValuesPlaceholder, ","),
	)
	if this.Debug {
		log.Println("Sql", s)
		log.Println(" Parameters", m)
	}
	result, err = db.NamedExec(s, *m)
	if err != nil {
		if errMysql, ok := err.(*mysql.MySQLError); ok {
			// duplicated record
			if errMysql.Number == 1062 {
				err = ErrDuplicatedUniqueKey
				return
			}
		}
	}

	return
}

func (this *DBWrapper) Del(db *sqlx.DB, pkName string, m *map[string]interface{}) (err error) {
	if db == nil {
		db, err = this.OpenDB()
		if err != nil {
			return
		}
		defer db.Close()
	}

	conditions := []string{}

	for k := range *m {
		conditions = append(conditions, fmt.Sprintf("%s=:%s", k, k))

	}

	s := fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT 1", this.TableName, strings.Join(conditions, " AND "))
	if this.Debug {
		log.Println("Sql", s)
		log.Println(" Parameters", m)
	}
	_, err = db.NamedExec(s, *m)
	return
}
