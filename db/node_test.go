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

func TestSetHeader(t *testing.T) {
	nodesTypes := []uint16{BNODE_NODE, BNODE_LEAF}
	nodesNkeys := []uint16{10, 20}
	for i := 0; i < len(nodesTypes); i++ {
		bnode := BNode{}
		bnode.InitData()
		bnode.setHeader(nodesTypes[i], nodesNkeys[i])
		assertEquals(t, nodesTypes[i], bnode.btype())
		assertEquals(t, nodesNkeys[i], bnode.nkeys())
	}
}

func TestPtrOperations(t *testing.T) {
	bnode := BNode{}
	bnode.InitData()
	bnode.setHeader(BNODE_NODE, 9)
	indexes := []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8}
	values := []uint64{10, 20, 30, 40, 50, 60, 70, 80, 90}

	for _, index := range indexes {
		bnode.setPtr(index, values[index])
		valueForIndex := bnode.getPtr(index)
		assertEquals(t, values[index], valueForIndex)
	}
}

func TestShouldThrowErrorWhenGettingPtrBiggerThanQtyKeys(t *testing.T) {
	bnode := BNode{}
	bnode.InitData()
	bnode.setHeader(BNODE_NODE, 2)

	defer func() {
		r := recover()

		if r == nil {
			t.Fatal("expected error")
		}

		if r != "index out of range" {
			t.Fatalf("Expected 'index out of range' error, got %v", r)
		}

	}()

	indexes := []uint16{0, 1, 2}
	values := []uint64{10, 20, 30}

	for _, index := range indexes {
		bnode.setPtr(index, values[index])
	}
}

func TestOffsetOperations(t *testing.T) {
	bnode := BNode{}
	bnode.InitData()
	bnode.setHeader(BNODE_NODE, 9)
	indexes := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	values := []uint16{10, 20, 30, 40, 50, 60, 70, 80, 90}

	for _, index := range indexes {
		bnode.setOffset(index, values[index])
		valueForIndex := bnode.getOffset(index)
		assertEquals(t, values[index], valueForIndex)
	}
}

func TestShouldThrowErrorWhenUsingInvalidIndexForOffset(t *testing.T) {
	bnode := BNode{}
	bnode.InitData()
	bnode.setHeader(BNODE_NODE, 2)

	defer func() {
		r := recover()

		if r == nil {
			t.Fatal("expected error")
		}

		if r != "index out of range" {
			t.Fatalf("Expected 'index out of range' error, got %v", r)
		}

	}()

	indexes := []uint16{0, 1, 2, 3}
	values := []uint16{10, 20, 30, 40}

	for _, index := range indexes {
		bnode.setOffset(index, values[index])
	}
}
