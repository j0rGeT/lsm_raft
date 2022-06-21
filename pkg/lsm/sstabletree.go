package lsm

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"log"
	"mylsmtree/pkg/config"
	"mylsmtree/pkg/kv"
	"mylsmtree/pkg/sort_tree"
	"mylsmtree/pkg/utils"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type TableTree struct {
	levels []*TableNode
	lock *sync.RWMutex
}

type TableNode struct {
	index int
	table *SSTable
	next *TableNode
}

func (tree *TableTree) loadDbFile(path string) {
	log.Println("loading the table tree")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("loading the table tree elapse time", elapse)
	}()

	level, index, err := utils.GetLevel(filepath.Base(path))
	if err != nil {
		panic(err)
	}

	table := &SSTable{}
	table.Init(path)
	newNode := &TableNode{
		index: index,
		table: table,
	}

	currentNode := tree.levels[level]
	if currentNode == nil {
		tree.levels[level] = newNode
		return
	}

	if newNode.index < currentNode.index {
		newNode.next = currentNode
		tree.levels[level] = newNode
		return
	}

	for currentNode != nil {
		if currentNode.next == nil || newNode.index < currentNode.next.index {
			newNode.next = currentNode.next
			currentNode.next = newNode
			break
		} else {
			currentNode = currentNode.next
		}
	}

}

func (tree *TableTree) insert(table *SSTable, level int) (index int) {

	tree.lock.Lock()
	defer tree.lock.Unlock()

	node := tree.levels[level]
	newNode := &TableNode{
		table: table,
		next: nil,
		index: 0,
	}

	if node == nil {
		tree.levels[level] = newNode
	}else {
		for node != nil {
			if node.next == nil {
				newNode.index = node.index + 1
				node.next = newNode
				break
			}else {
				node = node.next
			}
		}
	}
	return newNode.index
}

func (tree *TableTree) Search(key string) (kv.Value, kv.SearchResult){
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	for _, node := range tree.levels {
		tables := make([]*SSTable, 0)
		for node != nil {
			tables = append(tables, node.table)
			node = node.next
		}
		for i := len(tables) - 1; i >= 0; i-- {
			value, searchRsult := tables[i].Search(key)
			if searchRsult == kv.None {
				continue
			}else {
				return value, searchRsult
			}
		}
	}
	return kv.Value{}, kv.None
}

func (tree *TableTree) getMaxIndex(level int) int {
	node := tree.levels[level]
	index := 0
	for node != nil {
		index = node.index
		node = node.next
	}
	return index
}

func (tree *TableTree) getCount(level int) int {
	node := tree.levels[level]
	count := 0
	for node != nil {
		count ++
		node = node.next
	}
	return count
}

var levelMaxSize []int

func (tree *TableTree) Init(dir string) {
	log.Println("init table tree")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("sstable init elapse time", elapse)
	}()

	con := config.GetConfig()
	levelMaxSize = make([]int, 10)
	var i = 0
	for i < 10 {
		if i == 0 {
			levelMaxSize[i] = con.Level0Size
		} else {
			levelMaxSize[i] = levelMaxSize[i-1] * 10
		}
		i++
	}
	tree.levels = make([]*TableNode, 10)
	tree.lock = &sync.RWMutex{}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, info := range infos {
		if path.Ext(info.Name()) == ".db" {
			tree.loadDbFile(path.Join(dir, info.Name()))
		}
	}

}


func WriteDataToFile(filePath string, dataArea []byte, indexArea []byte, meta MetaInfo) {
	f, err := os.OpenFile(filePath, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0666)
	if err != nil {
		log.Println("error create file", err)
		return
	}

	_, err = f.Write(dataArea)
	if err != nil {
		log.Println("error write file", err)
		return
	}

	_, err = f.Write(indexArea)
	if err != nil {
		log.Println("err write index file", err)
		return
	}

	_ = binary.Write(f, binary.LittleEndian, &meta.version)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexLen)

	err = f.Sync()
	if err != nil {
		log.Println("error write file", err)
		return
	}

	err = f.Close()
	if err != nil {
		log.Println("error close file", err)
		return
	}
}

// GetLevelSize 获取指定层的 SSTable 总大小
func (tree *TableTree) GetLevelSize(level int) int64 {
	var size int64
	node := tree.levels[level]
	for node != nil {
		size += node.table.GetDbSize()
		node = node.next
	}
	return size
}

func (tree *TableTree) CreateNewTable(values []kv.Value) {
	tree.CreateTable(values, 0)
}

func (tree *TableTree) CreateTable(values []kv.Value, level int) *SSTable {
	keys := make([]string, 0, len(values))
	positions := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, value := range values {
		data, err := kv.Encode(value)
		if err != nil {
			log.Println("encode failure")
			continue
		}
		keys = append(keys, value.Key)
		positions[value.Key] = Position{
			Start: int64(len(dataArea)),
			Len: int64(len(data)),
			Deleted: value.Deleted,
		}
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Println("index generate failure")
		return nil
	}

	meta := MetaInfo{
		version: 0,
		dataStart: 0,
		dataLen: int64(len(dataArea)),
		indexStart: int64(len(dataArea)),
		indexLen: int64(len(indexArea)),
	}

	table := &SSTable{
		tableMetaInfo: meta,
		sparseIndex: positions,
		sortIndex: keys,
		lock: &sync.RWMutex{},
	}

	index := tree.insert(table,level)
	log.Println("create a new ss table")
	con := config.GetConfig()
	filePath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.filePath = filePath

	WriteDataToFile(filePath, dataArea, indexArea, meta)
	f , err := os.OpenFile(table.filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	table.f = f
	return table
}

func (tree *TableTree) Check() {
	tree.majorCompaction()
}

func (tree *TableTree) majorCompaction() {
	con := config.GetConfig()
	for levelIndex, _ := range tree.levels {
		tableSize := int(tree.GetLevelSize(levelIndex))
		if tree.getCount(levelIndex) > con.PartSize || tableSize > levelMaxSize[levelIndex] {
			tree.majorCompactionLevel(levelIndex)
		}
	}
}

func (tree *TableTree) majorCompactionLevel(level int) {
	log.Println("compresssing layer")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("compeled comporession", elapse)
	}()

	log.Printf("Compressing layer %d.db files\r\n", level)

	tableCache := make([]byte, levelMaxSize[level])
	currentNode := tree.levels[level]

	memoryTree := &sort_tree.Tree{}
	memoryTree.Init()

	tree.lock.Lock()
	for currentNode != nil {
		table := currentNode.table
		if int64(len(tableCache)) < table.tableMetaInfo.dataLen {
			tableCache = make([]byte, table.tableMetaInfo.dataLen)
		}

		newSlice := tableCache[0:table.tableMetaInfo.dataLen]

		if _, err := table.f.Seek(0, 0); err != nil {
			log.Println("error open file")
			panic(err)
		}

		if _, err := table.f.Read(newSlice); err != nil {
			panic(err)
		}

		for k, position := range table.sparseIndex {
			if position.Deleted == false {
				value, err := kv.Decode(newSlice)
				if err != nil {
					log.Fatal(err)
				}
				memoryTree.Set(k, value.Value)
			}else {
				memoryTree.Delete(k)
			}
		}
		currentNode = currentNode.next

	}

	tree.lock.Unlock()

	values := memoryTree.GetValues()
	newLevel := level + 1
	if newLevel > 10 {
		newLevel = 10
	}

	tree.CreateTable(values, newLevel)

	oldNode := tree.levels[level]
	if level < 10 {
		tree.levels[level] = nil
		tree.clearLevel(oldNode)
	}


}

func (tree *TableTree) clearLevel(oldNode *TableNode) {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	for oldNode != nil {
		err := oldNode.table.f.Close()
		if err != nil {
			panic(err)
		}

		err = os.Remove(oldNode.table.filePath)
		if err != nil {
			panic(err)
		}

		oldNode.table.f = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}

}








