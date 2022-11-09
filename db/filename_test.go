package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWALFileName(t *testing.T) {
	assert.Equal(t, WALFileName("database", 1), "database/1.wal")
}
