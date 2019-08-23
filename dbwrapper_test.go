// Simple tests.
// Setup database
//   grant all privileges on `test`.* to  'test'@'127.0.0.1' identified by 'test';
package dbwrapper

import (
	"log"
	"strings"
	"testing"
	"time"
)

var sqlCreateTest = `
CREATE TABLE IF NOT EXISTS test_dbwrapper (
	id int AUTO_INCREMENT,
	mobileNo varchar(11),
	password varchar(32),
	created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	lastModified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	UNIQUE KEY mobileNo (mobileNo),
	PRIMARY KEY (id)
);
`
var sqlDropTest = `DROP TABLE test_dbwrapper`

func tearDown(mgr *AccountProxy) {
	db, err := mgr.OpenDB()
	if err != nil {
		log.Fatalln("[faltal] mgr.OpenDB", err)
	}
	defer db.Close()
	_, err = db.Exec(sqlDropTest)
	if err != nil {
		if strings.Index(err.Error(), "Unknown table") != -1 {
			// drop a table not exists, safe to skip
		} else {
			log.Fatalln("[fatal] db.Exec", err)
		}
	}
}

func setUp(mgr *AccountProxy) {
	db, err := mgr.OpenDB()
	if err != nil {
		log.Fatalln("[faltal] mgr.OpenDB", err)
	}
	defer db.Close()
	_, err = db.Exec(sqlCreateTest)
	if err != nil {
		log.Fatalln("[fatal] db.Exec", err)
	}
}

type Account struct {
	ID           uint64    `json:"id" db:"id"`
	MobileNo     string    `json:"mobileNo" db:"mobileNo"`
	Password     string    `json:"password" db:"password"`
	Created      time.Time `json:"created" db:"created"`
	LastModified time.Time `json:"lastModified" db:"lastModified"`
}

type AccountProxy struct {
	DBWrapper
}

func NewAccountProxy() *AccountProxy {
	p := AccountProxy{}
	p.DriverName = "mysql"
	p.Debug = true
	p.Dsn = "test:test@tcp(127.0.0.1:3306)/test?charset=utf8mb4,utf8&timeout=2s&writeTimeout=2s&readTimeout=2s&parseTime=true"
	p.TableName = "test_dbwrapper"
	return &p
}

func TestCreate(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Llongfile)

	mgr := NewAccountProxy()
	db := mgr.MustOpenDB()
	defer db.Close()

	tearDown(mgr)
	setUp(mgr)

	a := Account{}
	mobileNo := "13800138000"

	// Test Read
	err := mgr.Get(db, &a, []string{"mobileNo", "id"}, "id", 1)

	if err != ErrRecordNotFound {
		t.Errorf("expected Mgr.Get() returns err != ErrRecordNotFound, got %v", err)
	}

	// Test Create
	result, err := mgr.Create(db, &map[string]interface{}{
		"mobileNo": "13800138000",
	})
	if err != nil {
		t.Errorf("expected mgr.CreateOrUpdate() returns err==nil, got %v", err)
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil || lastInsertID <= 0 {
		t.Errorf("expected mgr.LastInsertId() LastInsertId > 0, got lastInsertId=%d err=%v", lastInsertID, err)
	}

	err = mgr.Get(db, &a, []string{"mobileNo", "id"}, "id", lastInsertID)
	if err == ErrRecordNotFound {
		t.Errorf("expected Mgr.Get() returns err == ErrRecordNotFound, got %v", err)
	}
	if a.MobileNo != mobileNo || a.Password != "" {
		t.Errorf(`expected Mgr.Get() returns password == "", got %v`, a.Password)
	}

	_, err = mgr.Create(db, &map[string]interface{}{
		"mobileNo": "13800138000",
	})
	if err == nil {
		t.Errorf("expected mgr.CreateOrUpdate() returns err!=nil, got nil")
	}

	tearDown(mgr)

}

func TestCRUD(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Llongfile)

	mgr := NewAccountProxy()
	db := mgr.MustOpenDB()
	defer db.Close()

	tearDown(mgr)
	setUp(mgr)

	a := Account{}
	mobileNo := "13800138000"

	// Test Read
	err := mgr.Get(db, &a, []string{"mobileNo", "id"}, "id", 1)

	if err != ErrRecordNotFound {
		t.Errorf("expected Mgr.Get() returns err != ErrRecordNotFound, got %v", err)
	}

	// Test Create
	result, err := mgr.CreateOrUpdate(db, &map[string]interface{}{
		"mobileNo": "13800138000",
	})
	if err != nil {
		t.Errorf("expected mgr.CreateOrUpdate() returns err==nil, got %v", err)
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil || lastInsertID <= 0 {
		t.Errorf("expected mgr.LastInsertId() LastInsertId > 0, got lastInsertId=%d err=%v", lastInsertID, err)
	}

	err = mgr.Get(db, &a, []string{"mobileNo", "id"}, "id", lastInsertID)
	if err == ErrRecordNotFound {
		t.Errorf("expected Mgr.Get() returns err == ErrRecordNotFound, got %v", err)
	}
	if a.MobileNo != mobileNo || a.Password != "" {
		t.Errorf(`expected Mgr.Get() returns password == "", got %v`, a.Password)
	}

	// Test Update
	passwordNew := "secret"
	result, err = mgr.CreateOrUpdate(db, &map[string]interface{}{
		"id":       lastInsertID,
		"password": passwordNew,
	})

	lastInsertID, err = result.LastInsertId()
	if err != nil || lastInsertID <= 0 {
		t.Errorf(`expected Mgr.CreateOrUpdate() returns err == nil, lastInsertID > 0, got %v, %v`, err, lastInsertID)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Errorf(`expected Mgr.CreateOrUpdate() returns err == nil, rowsAffected > 0, got %v, %v`, err, rowsAffected)
	}

	b := Account{}
	err = mgr.Get(db, &b, []string{"id", "mobileNo", "password"}, "id", lastInsertID)
	if err == ErrRecordNotFound {
		t.Errorf("expected Mgr.CreateOrUpdate() returns err != ErrRecordNotFound, got %v", err)
	}
	if b.Password != passwordNew {
		t.Errorf("expected Mgr.CreateOrUpdate() returns %s, got %v", passwordNew, b.Password)
	}

	// Test Delete
	err = mgr.Del(db, "id", &map[string]interface{}{
		"id": lastInsertID,
	})
	if err != nil {
		t.Errorf("expected Mgr.Del() returns err==nil, got %v", err)
	}

	err = mgr.Get(db, &a, []string{"mobileNo", "id"}, "id", lastInsertID)
	if err != ErrRecordNotFound {
		t.Errorf("expected Mgr.Get() returns err != ErrRecordNotFound, got %v", err)
	}

	tearDown(mgr)
}

func TestSearch(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Llongfile)

	mgr := NewAccountProxy()
	db := mgr.MustOpenDB()
	defer db.Close()

	tearDown(mgr)
	setUp(mgr)

	a := Account{}
	mobileNo := "13800138000"

	// Test Read
	err := mgr.Get(db, &a, []string{"mobileNo", "id"}, "id", 1)

	if err != ErrRecordNotFound {
		t.Errorf("expected Mgr.Get() returns err != ErrRecordNotFound, got %v", err)
	}

	// Test Create
	result, err := mgr.CreateOrUpdate(db, &map[string]interface{}{
		"mobileNo": "13800138000",
	})
	if err != nil {
		t.Errorf("expected mgr.CreateOrUpdate() returns err==nil, got %v", err)
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil || lastInsertID <= 0 {
		t.Errorf("expected mgr.LastInsertId() LastInsertId > 0, got lastInsertId=%d err=%v", lastInsertID, err)
	}

	err = mgr.Get(db, &a, []string{"mobileNo", "id"}, "id", lastInsertID)
	if err == ErrRecordNotFound {
		t.Errorf("expected Mgr.Get() returns err == ErrRecordNotFound, got %v", err)
	}
	if a.MobileNo != mobileNo || a.Password != "" {
		t.Errorf(`expected Mgr.Get() returns password == "", got %v`, a.Password)
	}

	limit := 10
	accounts := []Account{}
	err = mgr.Search(db, &accounts, []string{"id", "mobileNo"}, &map[string]interface{}{}, &map[string]interface{}{
		"mobileNo": "1380",
	}, limit)

	if err != nil {
		t.Errorf("expected Mgr.Search() returns err == nil, got %v", err)
	}

	if len(accounts) == 0 {
		t.Errorf("expected Mgr.Search() returns len(records)>0, got 0")
	} else if accounts[0].MobileNo != mobileNo {
		t.Errorf("expected Mgr.Search() returns records[0].MobileNo == %s, got %v", mobileNo, accounts[0].MobileNo)
	}

	accountsMiss := []Account{}
	err = mgr.Search(db, &accountsMiss, []string{"id", "mobileNo"}, &map[string]interface{}{}, &map[string]interface{}{
		"mobileNo": "8888",
	}, limit)

	if err != nil {
		t.Errorf("expected Mgr.Search() returns err == nil, got %v", err)
	}

	if len(accountsMiss) != 0 {
		t.Errorf("expected Mgr.Search() returns len(records)==0, got >0")
	}

	tearDown(mgr)
}
