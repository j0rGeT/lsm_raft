package config

import "sync"

type Config struct {
	DataDir string
	Level0Size int
	PartSize int
	Threshold int
	CheckInterval int
}

var once *sync.Once = &sync.Once{}

var config Config

func Init(con Config) {
	once.Do(func() {
		config = con
	})
}

func GetConfig() Config {
	return config
}