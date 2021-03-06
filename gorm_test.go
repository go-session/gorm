package gorm

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-session/session"

	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	expired = 1
)

func TestSqliteStore(t *testing.T) {
	dsn := os.TempDir() + "/gorm.db"
	store, err := NewStore(Config{GCInterval: 1}, "sqlite3", dsn)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer store.Close()

	Convey("Test gorm sqlite store operation", t, func() {
		testStore(t, store)
		testManagerStore(t, store)
		testGC(t, store)
	})
}

func TestMySQLStore(t *testing.T) {
	dsn := "root:@tcp(127.0.0.1:3306)/myapp_test?charset=utf8&parseTime=True&loc=Local"
	store, err := NewStore(Config{GCInterval: 1}, "mysql", dsn)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer store.Close()

	Convey("Test gorm mysql store operation", t, func() {
		testStore(t, store)
		testManagerStore(t, store)
		testGC(t, store)
	})
}

func newSid() string {
	return "test_gorm_store_" + time.Now().String()
}

func testStore(t *testing.T, mstore session.ManagerStore) {
	ctx := context.Background()
	sid := newSid()
	defer mstore.Delete(ctx, sid)

	store, err := mstore.Create(ctx, sid, expired)
	So(err, ShouldBeNil)
	foo, ok := store.Get("foo")
	So(ok, ShouldBeFalse)
	So(foo, ShouldBeNil)

	store.Set("foo", "bar")
	store.Set("foo2", "bar2")
	err = store.Save()
	So(err, ShouldBeNil)

	foo, ok = store.Get("foo")
	So(ok, ShouldBeTrue)
	So(foo, ShouldEqual, "bar")

	foo = store.Delete("foo")
	So(foo, ShouldEqual, "bar")

	foo, ok = store.Get("foo")
	So(ok, ShouldBeFalse)
	So(foo, ShouldBeNil)

	foo2, ok := store.Get("foo2")
	So(ok, ShouldBeTrue)
	So(foo2, ShouldEqual, "bar2")

	err = store.Flush()
	So(err, ShouldBeNil)

	foo2, ok = store.Get("foo2")
	So(ok, ShouldBeFalse)
	So(foo2, ShouldBeNil)
}

func testManagerStore(t *testing.T, mstore session.ManagerStore) {
	ctx := context.Background()
	sid := newSid()
	store, err := mstore.Create(ctx, sid, expired)
	So(store, ShouldNotBeNil)
	So(err, ShouldBeNil)

	store.Set("foo", "bar")
	err = store.Save()
	So(err, ShouldBeNil)

	store, err = mstore.Update(ctx, sid, expired)
	So(store, ShouldNotBeNil)
	So(err, ShouldBeNil)

	foo, ok := store.Get("foo")
	So(ok, ShouldBeTrue)
	So(foo, ShouldEqual, "bar")

	newsid := newSid()
	store, err = mstore.Refresh(ctx, sid, newsid, expired)
	So(store, ShouldNotBeNil)
	So(err, ShouldBeNil)

	foo, ok = store.Get("foo")
	So(ok, ShouldBeTrue)
	So(foo, ShouldEqual, "bar")

	exists, err := mstore.Check(ctx, sid)
	So(exists, ShouldBeFalse)
	So(err, ShouldBeNil)

	err = mstore.Delete(ctx, newsid)
	So(err, ShouldBeNil)

	exists, err = mstore.Check(ctx, newsid)
	So(exists, ShouldBeFalse)
	So(err, ShouldBeNil)
}

func testGC(t *testing.T, mstore session.ManagerStore) {
	ctx := context.Background()
	sid := newSid()
	store, err := mstore.Create(ctx, sid, expired)
	So(store, ShouldNotBeNil)
	So(err, ShouldBeNil)

	store.Set("foo", "bar")
	err = store.Save()
	So(err, ShouldBeNil)

	foo, ok := store.Get("foo")
	So(ok, ShouldBeTrue)
	So(foo, ShouldEqual, "bar")

	time.Sleep(time.Second * 2)

	exists, err := mstore.Check(ctx, sid)
	So(exists, ShouldBeFalse)
	So(err, ShouldBeNil)
}
