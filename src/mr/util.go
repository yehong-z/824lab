package mr

import (
	"io"
	"os"
)

func GetContent(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		//log.Fatalf("cannot open %v", filename)
		return "", err
	}

	content, err := io.ReadAll(file)
	if err != nil {
		//log.Fatalf("cannot read %v", filename)
		return "", err
	}
	file.Close()

	return string(content), nil
}
