package engine

import "os"

func osStat(path string) (os.FileInfo, error) { return os.Stat(path) }
