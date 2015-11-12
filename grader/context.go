package grader

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lhchavez/quark/common"
	_ "github.com/mattn/go-sqlite3"
)

type Context struct {
	common.Context
	DB *sql.DB
}

func NewContext(configPath string) (*Context, error) {
	ctx, err := common.NewContext(configPath)
	if err != nil {
		return nil, err
	}
	var context = &Context{
		Context: *ctx,
	}

	// Database
	context.DB, err = sql.Open(context.Config.Db.Driver,
		context.Config.Db.DataSourceName)
	if err != nil {
		return nil, err
	}
	if err := context.DB.Ping(); err != nil {
		return nil, err
	}

	return context, nil
}

// Close releases all resources owned by the context.
func (context *Context) Close() {
	context.DB.Close()
}
