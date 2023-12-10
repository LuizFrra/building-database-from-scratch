package db

import (
	"testing"
)

func assertEquals(t *testing.T, expected interface{}, actual interface{}) {
	if expected != actual {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}
}

func TestIniteData(t *testing.T) {
	bnode := BNode{}
	assertEquals(t, 0, len(bnode.data))
	bnode.InitData()
	assertEquals(t, 4096, len(bnode.data))
}
