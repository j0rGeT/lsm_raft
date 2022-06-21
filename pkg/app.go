package pkg

import (
	"flag"
	"fmt"
	"github.com/hashicorp/raft"
	"log"
	"mylsmtree/pkg/config"
	"mylsmtree/pkg/lsm"
	"mylsmtree/pkg/myraft"
	"mylsmtree/pkg/sort_tree"
	"mylsmtree/pkg/wal"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

type AppConfig struct{}

func Check() {
	con := config.GetConfig()
	ticker := time.Tick(time.Duration(con.CheckInterval) * time.Second)
	for _ = range ticker {
		log.Println("Performing background checks...")
		// 检查内存
		checkMemory()
		// 检查压缩数据库文件
		database.TableTree.Check()
	}
}

func checkMemory() {
	con := config.GetConfig()
	count := database.MemoryTree.GetCount()
	if count < con.Threshold {
		return
	}
	// 交互内存
	log.Println("Compressing memory")
	tmpTree := database.MemoryTree.Swap()

	// 将内存表存储到 SsTable 中
	database.TableTree.CreateNewTable(tmpTree.GetValues())
	database.Wal.Reset()
}

// 初始化 Database，从磁盘文件中还原 SSTable、WalF、内存表等
func initDatabase(dir string) {
	database = &Database{
		MemoryTree: &sort_tree.Tree{},
		Wal:        &wal.Wal{},
		TableTree:  &lsm.TableTree{},
	}
	// 从磁盘文件中恢复数据
	// 如果目录不存在，则为空数据库
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Failed to create the database directory")
			panic(err)
		}
	}
	// 从数据目录中，加载 WalF、database 文件
	// 非空数据库，则开始恢复数据，加载 WalF 和 SSTable 文件
	memoryTree := database.Wal.Init(dir)

	database.MemoryTree = memoryTree
	log.Println("Loading database...")
	database.TableTree.Init(dir)
}

var (
	httpAddr    string
	raftAddr    string
	raftId      string
	raftCluster string
	raftDir     string
)

var (
	isLeader int64
)

func init() {
	flag.StringVar(&httpAddr, "http_addr", "127.0.0.1:7001", "http listen addr")
	flag.StringVar(&raftAddr, "raft_addr", "127.0.0.1:7000", "raft listen addr")
	flag.StringVar(&raftId, "raft_id", "1", "raft id")
	flag.StringVar(&raftCluster, "raft_cluster", "1/127.0.0.1:7000", "cluster info")
}

// Start 启动
func StartServer(con config.Config) {
	if database != nil {
		return
	}
	// 将配置保存到内存中
	log.Println("Loading a Configuration File")
	config.Init(con)
	// 初始化数据库
	log.Println("Initializing the database")
	initDatabase(con.DataDir)

	// 数据库启动前进行一次数据压缩
	log.Println("Performing background checks...")
	// 检查内存
	checkMemory()
	// 检查压缩数据库文件
	database.TableTree.Check()
	// 启动后台线程
	go Check()

	flag.Parse()
	if httpAddr == "" || raftAddr == "" || raftId == "" || raftCluster == "" {
		fmt.Println("config error")
		os.Exit(1)
		return
	}

	raftDir := "node/raft_" + raftId
	os.MkdirAll(raftDir, 0700)

	// 初始化raft
	myRaft, fm, err := myraft.NewMyRaft(raftAddr, raftId, raftDir)
	if err != nil {
		fmt.Println("NewMyRaft error ", err)
		os.Exit(1)
		return
	}

	// 启动raft
	myraft.Bootstrap(myRaft, raftId, raftAddr, raftCluster)

	// 监听leader变化（使用此方法无法保证强一致性读，仅做leader变化过程观察）
	go func() {
		for leader := range myRaft.LeaderCh() {
			if leader {
				atomic.StoreInt64(&isLeader, 1)
			} else {
				atomic.StoreInt64(&isLeader, 0)
			}
		}
	}()

	// 启动http server
	httpServer := HttpServer{
		ctx: myRaft,
		fsm: fm,
	}

	http.HandleFunc("/set", httpServer.Set)
	http.HandleFunc("/get", httpServer.Get)
	http.ListenAndServe(httpAddr, nil)

	// 关闭raft
	shutdownFuture := myRaft.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		fmt.Printf("shutdown raft error:%v \n", err)
	}
	// 退出http server
	fmt.Println("shutdown kv http server")

}


type HttpServer struct {
	ctx *raft.Raft
	fsm *myraft.Fsm
}

func (h HttpServer) Set(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt64(&isLeader) == 0 {
		fmt.Fprintf(w, "not leader")
		return
	}
	vars := r.URL.Query()
	key := vars.Get("key")
	value := vars.Get("value")
	flag := Set(key, value)
	if flag {
		fmt.Fprintf(w, "success")
	}else {
		fmt.Fprintf(w, "failure")
	}
	return
}

func (h HttpServer) Get(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()
	key := vars.Get("key")
	val, flag := Get(key)
	if flag {
		fmt.Fprintf(w, fmt.Sprintf("result is %v", val))
	}else {
		fmt.Fprintf(w, fmt.Sprintf("result is %v", val))
	}
	return
}
