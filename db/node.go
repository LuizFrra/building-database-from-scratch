package db

import (
	"encoding/binary"
)

/*
A node has:
1. header that contains the type of the node and the number of keys
2. list of pointers to the child nodes
3. list of offset point to each key-value pair
4. packed KV pairs
*/

type BNode struct {
	data []byte // data that will be dumped to the disk
}

const (
	BNODE_NODE           = 1 // indicates that this is an internal node
	BNODE_LEAF           = 2 // indicade that this is a leaf node
	BTREE_OFFSET_SIZE    = 2
	BTREE_KLEN_SIZE      = 2 // size of the key length
	BTREE_KVAL_SIZE      = 2 // size of the value length
	BTREE_KEY_LEN_SIZE   = 4 // how much space the key takes
	BTREE_KEY_VALUE_SIZE = 4 // how much space the value takes
	HEADER               = 4
	POINTER_SIZE         = 8
	BTREE_PAGE_SIZE      = 4096 // size of a page, or single node
	BTREE_MAX_KEY_SIZE   = 1000
	BTREE_MAX_VAL_SIZE   = 3000
)

type Btree struct {
	root uint64 // a nonzero page number

	get func(uint64) BNode // get page
	new func(BNode) uint64 // allocate new page
	del func(uint64)       // delete page
}

/* PACKAGE HEADER OPERATIONS */

func (node BNode) btype() uint16 {
	btypeStart := 0
	btypeEnd := 2
	return binary.LittleEndian.Uint16(node.data[btypeStart:btypeEnd])
}

func (node BNode) nkeys() uint16 {
	nkeysStart := 2
	nkeysEnd := 4
	return binary.LittleEndian.Uint16(node.data[nkeysStart:nkeysEnd])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

/* PTR OPERATIONS */

func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	pos := HEADER + (idx * POINTER_SIZE)
	return binary.LittleEndian.Uint64(node.data[pos : pos+POINTER_SIZE])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	pos := HEADER + (idx * POINTER_SIZE)
	binary.LittleEndian.PutUint64(node.data[pos:pos+POINTER_SIZE], val)
}

/* OFFSET OPERATIONS */

// offset is the distance of the beggining of the block to the start of a specific data

func offsetPos(node BNode, idx uint16) uint16 {
	if idx < 1 || idx > node.nkeys() {
		panic("index out of range")
	}
	return HEADER + (POINTER_SIZE * node.nkeys()) + (BTREE_OFFSET_SIZE * (idx - 1))
}

func (node BNode) getOffset(idx uint16) uint16 {
	// if idx is 0, it means that the offset is 0
	if idx == 0 {
		return 0
	}
	offsetPosNum := offsetPos(node, idx)
	return binary.LittleEndian.Uint16(node.data[offsetPosNum : offsetPosNum+BTREE_OFFSET_SIZE])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	offsetPosNum := offsetPos(node, idx)
	binary.LittleEndian.PutUint16(node.data[offsetPosNum:offsetPosNum+BTREE_OFFSET_SIZE], offset)
}

/* KV OPERATIONS */

func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() { // se idx for maior q nkeys, idx esta fora do range
		panic("index out of range")
	}
	pos := HEADER + (POINTER_SIZE * node.nkeys()) + (BTREE_OFFSET_SIZE * node.nkeys()) + (node.getOffset(idx))
	return pos
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	// where the KV pair starts
	pos := node.kvPos(idx)
	// get the key size
	klen := binary.LittleEndian.Uint16(node.data[pos : pos+BTREE_KLEN_SIZE])
	// where the key value begins: KV PAIR STARTS + KEY SIZE + VALUE SIZE
	keylenStart := pos + (BTREE_KLEN_SIZE + BTREE_KVAL_SIZE)
	return node.data[keylenStart : keylenStart+klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("index out of range")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos : pos+BTREE_KLEN_SIZE])
	vlen := binary.LittleEndian.Uint16(node.data[pos+BTREE_KLEN_SIZE : pos+BTREE_KLEN_SIZE+BTREE_KVAL_SIZE])
	valLenstart := pos + (BTREE_KLEN_SIZE + BTREE_KVAL_SIZE + klen)
	return node.data[valLenstart : valLenstart+vlen]
}

/* NODE OPERATION */

func (node *BNode) InitData() {
	node.data = make([]byte, BTREE_PAGE_SIZE)
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
