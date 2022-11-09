package db

import (
	"fmt"
)

func makeFileName(dbName string, number uint64, suffix string) string {
	return fmt.Sprintf("%s/%d.%s", dbName, number, suffix)
}

func WALFileName(dbName string, number uint64) string {
	return makeFileName(dbName, number, "wal")
}

func SSTFileName(dbName string, number uint64) string {
	return makeFileName(dbName, number, "sst")
}
