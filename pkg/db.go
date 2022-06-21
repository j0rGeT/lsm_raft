package pkg

import (
	"encoding/json"
	"log"
	"mylsmtree/pkg/kv"
)

// Get 获取一个元素
// 需要支持集群模式
func Get(key string) (interface{}, bool) {
	log.Print("Get ", key)
	// 先查内存表
	value, result := database.MemoryTree.Search(key)

	if result == kv.Success {
		return getInstance(value.Value)
	}

	// 查 SsTable 文件
	if database.TableTree != nil {
		value, result := database.TableTree.Search(key)
		if result == kv.Success {
			return getInstance(value.Value)
		}
	}
	var nilV interface{}
	return nilV, false
}

// Set 插入元素
// 只需要支持集群模式
func Set(key string, value interface{}) bool {
	log.Print("Insert ", key, ",")
	data, err := kv.Convert(value)
	if err != nil {
		log.Println(err)
		return false
	}

	_, _ = database.MemoryTree.Set(key, data)

	// 写入 wal.log
	database.Wal.Write(kv.Value{
		Key:     key,
		Value:   data,
		Deleted: false,
	})
	return true
}

// DeleteAndGet 删除元素并尝试获取旧的值，
// 返回的 bool 表示是否有旧值，不表示是否删除成功
func DeleteAndGet(key string) (interface{}, bool) {
	log.Print("Delete ", key)
	value, success := database.MemoryTree.Delete(key)

	if success {
		// 写入 wal.log
		database.Wal.Write(kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		})
		return getInstance(value.Value)
	}
	var nilV interface{}
	return nilV, false
}

// Delete 删除元素
func Delete(key string) {
	log.Print("Delete ", key)
	database.MemoryTree.Delete(key)
	database.Wal.Write(kv.Value{
		Key:     key,
		Value:   nil,
		Deleted: true,
	})
}

// 将字节数组转为类型对象
func getInstance(data []byte) (interface{}, bool) {
	var value interface{}
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Println(err)
	}
	return value, true
}

