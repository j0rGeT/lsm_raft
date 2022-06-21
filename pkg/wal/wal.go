package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"mylsmtree/pkg/kv"
	"mylsmtree/pkg/sort_tree"
	"os"
	"path"
	"sync"
	"time"
)

type Wal struct {
	f *os.File
	path string
	lock sync.Locker
}

func (w *Wal) Init(dir string) *sort_tree.Tree {
	log.Println("loading wal log")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("loades wal log consumption: ", elapse)
	}()

	walPath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Println("the wal log file cannot create")
		return nil
	}
	w.f = f
	w.path = walPath
	w.lock = &sync.Mutex{}
	return w.loadToMemory()
}

func (w *Wal) loadToMemory() *sort_tree.Tree {
	w.lock.Lock()
	defer w.lock.Unlock()

	info, _ := os.Stat(w.path)
	size := info.Size()
	tree := &sort_tree.Tree{}
	tree.Init()

	if size == 0 {
		return tree
	}

	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Println("failure to open the wal log")
		return nil
	}

	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("failure to open wal to send")
			panic(err)
		}
	}(w.f, size-1, 0)

	data := make([]byte, size)
	_, err = w.f.Read(data)
	if err != nil {
		log.Println("fail to open file to read")
		return nil
	}


	dataLen := int64(0)
	index := int64(0)
	for index < size {
		indexData := data[index:(index+8)]
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			panic(err)
		}
		index += 8
		dataArea := data[index:(index+dataLen)]
		var value kv.Value
		err = json.Unmarshal(dataArea, &value)
		if err != nil {
			panic(err)
		}

		if value.Deleted {
			tree.Delete(value.Key)
		}else {
			tree.Set(value.Key, value.Value)
		}
		index = index + dataLen

	}
	return tree
}

func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if value.Deleted {
		log.Println(" wal log delete", value.Key)
	} else {
		log.Println("wal log insert", value.Key)
	}

	data, _ := json.Marshal(value)
	err := binary.Write(w.f, binary.LittleEndian, int64(len(data)))
	if err != nil {
		panic(err)
	}

	err = binary.Write(w.f, binary.LittleEndian, data)
	if err != nil {
		panic(err)
	}
}

func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	log.Println("reset wal long file")
	err := w.f.Close()
	if err != nil {
		panic(err)
	}
	w.f = nil
	err = os.Remove(w.path)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	w.f = f
}














