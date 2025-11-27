package database

import (
	"database/sql"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
)

type DatabaseExecute interface {
	Connect(config v1.DatabaseConfig, password string) (*sql.DB, error)
	ExecuteQuery(db *sql.DB, query string) (*sql.Rows, error)
	ExecuteStatement(db *sql.DB, query string) (sql.Result, error)
	BeginTransaction(db *sql.DB) (*sql.Tx, error)
	Close(db *sql.DB) error
	ValidateConnection(db *sql.DB) error
	GetDatabaseVersion(db *sql.DB, dbType v1.DatabaseType) (string, error)
	IsQueryReadOnly(query string) bool
}
