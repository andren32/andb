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

func CurrentFileName(dbName string) string {
	return fmt.Sprintf("%s/CURRENT", dbName)
}

func ManifestFileName(dbName string, number uint64) string {
	return fmt.Sprintf("%s/%s", dbName, ManifestFileNameWithoutDBPath(number))
}

func ManifestFileNameWithoutDBPath(number uint64) string {
	return fmt.Sprintf("MANIFEST-%d", number)
}
