package main

import (
	"bufio"
	"fmt"
	"mylsmtree/pkg"
	"mylsmtree/pkg/config"
	"os"
)

func main() {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	pkg.StartServer(config.Config{
		DataDir:       `/Users/xudong/GolandProjects/mylsmtree/data`,
		Level0Size:    100,
		PartSize:      4,
		Threshold:     3000,
		CheckInterval: 3,
	})
}