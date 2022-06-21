package utils

import (
	"errors"
	"fmt"
)

func GetLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, errors.New("can not get level")
	}
	return level, index, nil
}
