package lsm

import (
	"encoding/binary"
	"encoding/json"
	"mylsmtree/pkg/kv"
	"os"
	"sort"
	"sync"
)

type MetaInfo struct {
	version int64
	dataStart int64
	dataLen int64
	indexStart int64
	indexLen int64
}

type Position struct {
	Start int64
	Len int64
	Deleted bool
}

type SSTable struct {
	f *os.File
	filePath string
	tableMetaInfo MetaInfo
	sparseIndex map[string]Position
	sortIndex []string
	lock sync.Locker
}

func (table *SSTable) Init(path string) {
	table.filePath = path
	table.lock = &sync.Mutex{}
	table.loadFileHandle()
}

func (table *SSTable) loadFileHandle() {
	if table.f == nil {
		f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
		if err != nil {
			panic(err)
		}

		table.f = f
	}
	table.loadMetaInfo()
	table.loadSparseIndex()
}

func (table *SSTable) loadMetaInfo() {
	f := table.f
	_, err := f.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	info, _ := f.Stat()
	_, err = f.Seek(info.Size()-8*5, 0)
	if err != nil {
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, table.tableMetaInfo.version)
	_, err = f.Seek(info.Size() - 8*4, 0)
	if err != nil {
		panic(err)
	}

	_ = binary.Read(f, binary.LittleEndian, table.tableMetaInfo.dataStart)

	_, err = f.Seek(info.Size() - 8 *3, 0)
	if err != nil {
		panic(err)
	}

	_ = binary.Read(f, binary.LittleEndian, table.tableMetaInfo.dataLen)

	_, err = f.Seek(info.Size() - 8*2, 0)
	if err != nil {
		panic(err)
	}

	_ = binary.Read(f, binary.LittleEndian, table.tableMetaInfo.indexStart)

	_, err  = f.Seek(info.Size() - 8, 0)
	if err != nil {
		panic(err)
	}

	_ = binary.Read(f, binary.LittleEndian, table.tableMetaInfo.indexLen)

}

func (table *SSTable) loadSparseIndex() {
	bytes := make([]byte, table.tableMetaInfo.indexLen)
	if _, err := table.f.Seek(table.tableMetaInfo.indexStart, 0); err != nil {
		panic(err)
	}
	if _, err := table.f.Read(bytes); err != nil {
		panic(err)
	}

	table.sparseIndex = make(map[string]Position)
	err := json.Unmarshal(bytes, &table.sparseIndex)
	if err != nil {
		panic(err)
	}

	_, _ = table.f.Seek(0, 0)
	keys := make([]string, 0, len(table.sparseIndex))
	for k := range table.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	table.sortIndex = keys

}

func (table *SSTable) GetDbSize() int64 {
	info, err := os.Stat(table.filePath)
	if err != nil {
		panic(err)
	}
	return info.Size()
}


func (table *SSTable) Search(key string) (value kv.Value, result kv.SearchResult) {
	table.lock.Lock()
	defer table.lock.Unlock()

	var position = Position{
		Start:  -1,
	}

	l := 0
	r := len(table.sortIndex) - 1

	for l <= r {
		mid := (l+r)/2
		if table.sortIndex[mid] == key {
			position = table.sparseIndex[key]
			if position.Deleted {
				return kv.Value{}, kv.Deleted
			}
		}else if table.sortIndex[mid] < key {
			l = mid + 1
		}else if table.sortIndex[mid] > key {
			r = mid - 1
		}
	}

	if position.Start == -1 {
		return kv.Value{}, kv.None
	}

	bytes := make([]byte, position.Len)
	if _, err := table.f.Seek(position.Start, 0); err != nil {
		return kv.Value{}, kv.None
	}

	if _, err := table.f.Read(bytes); err != nil {
		return kv.Value{}, kv.None
	}

	value, err := kv.Decode(bytes)
	if err != nil {
		return kv.Value{}, kv.None
	}
	return value, kv.Success
}






