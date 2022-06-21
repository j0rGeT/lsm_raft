package pkg

import (
	"mylsmtree/pkg/lsm"
	"mylsmtree/pkg/sort_tree"
	"mylsmtree/pkg/wal"
)

type Database struct {
	// 内存表
	MemoryTree *sort_tree.Tree
	// SSTable 列表
	TableTree *lsm.TableTree
	// WalF 文件句柄
	Wal *wal.Wal
}

// 数据库，全局唯一实例
var database *Database


