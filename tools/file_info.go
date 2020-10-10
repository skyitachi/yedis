package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	PAGE_SIZE            = 4096
	MetaPageIdOffset     = 0
	MetaRootPageIdOffset = 4
	MetaLevelOffset      = 8

	EntryCountOffset = 4
	AvailableOffset  = 12
	FlagOffset       = 16
	EntryOffset      = 29
)

type Entry struct {
	Key   int64
	VLen  uint32
	Value []byte
}

func ReadEntry(reader *bytes.Reader) (*Entry, error) {
	entry := &Entry{}
	var key int64
	err := binary.Read(reader, binary.LittleEndian, &key)
	if err != nil {
		return nil, err
	}
	log.Println("read entry key: ", key)
	entry.Key = key
	var vLen uint32
	err = binary.Read(reader, binary.LittleEndian, &vLen)
	if err != nil {
		return nil, err
	}
	entry.VLen = vLen
	entry.Value = make([]byte, vLen)
	n, err := reader.Read(entry.Value)
	if err != nil {
		return nil, err
	}
	if n != int(vLen) {
		return nil, fmt.Errorf("not enough data: vLen=%d, read=%d", vLen, n)
	}
	return entry, nil
}

// TODO: flag的位置要调整
func ReadIndexNode(reader *bytes.Reader, count int, degree int) (keys []int64, childs []int32, err error) {
	keyBytes := make([]byte, count*8)
	_, err = reader.Read(keyBytes)
	if err != nil {
		return nil, nil, err
	}
	buf := bytes.NewBuffer(keyBytes)
	for i := 0; i < count; i++ {
		var ret int64
		if err = binary.Read(buf, binary.LittleEndian, &ret); err != nil {
			log.Println(err)
			return nil, nil, err
		}
		keys = append(keys, ret)
	}
	_, err = reader.Seek(int64((degree-count)*8), io.SeekCurrent)
	if err != nil {
		return nil, nil, err
	}
	childBytes := make([]byte, (count+1)*4)
	_, err = reader.Read(childBytes)
	buf = bytes.NewBuffer(childBytes)
	for i := 0; i <= count; i++ {
		var ret int32
		if err = binary.Read(buf, binary.LittleEndian, &ret); err != nil {
			log.Println(err)
			return nil, nil, err
		}
		childs = append(childs, ret)
	}
	return

}

func main() {
	btreeFile, err := os.Open("btree.idx")
	if err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, PAGE_SIZE)
	// meta info checker
	n, err := btreeFile.Read(buf)
	if err != nil {
		log.Fatal(err)
	}
	if n != PAGE_SIZE {
		log.Fatalf("read size: %+v", n)
	}
	// read meta_page
	// page_id
	var meta_page_id int32
	metaReader := bytes.NewReader(buf)
	err = binary.Read(metaReader, binary.LittleEndian, &meta_page_id)
	if err != nil {
		log.Fatalf("read meta page id err: %+v", err)
	}
	log.Println("meta_page_id is ", meta_page_id)
	var root_page_id int32
	err = binary.Read(metaReader, binary.LittleEndian, &root_page_id)
	if err != nil {
		log.Fatalf("read root_page_id error: %+v", err)
	}
	log.Println("root_page_id is ", root_page_id)

	var levels int32
	err = binary.Read(metaReader, binary.LittleEndian, &levels)
	if err != nil {
		log.Fatalf("read level error :%+v", err)
	}
	log.Println("levels is: ", levels)

	// read root_page
	n, err = btreeFile.ReadAt(buf, int64(root_page_id*PAGE_SIZE))
	if err != nil {
		log.Fatalf("read root page error %+v", err)
	}
	rootReader := bytes.NewReader(buf)
	// parse page_id
	var real_page_id int32
	err = binary.Read(rootReader, binary.LittleEndian, &real_page_id)
	if err != nil {
		log.Fatal(err)
	}
	if real_page_id != root_page_id {
		log.Fatalf("meta root_page_id=%+v, real_page_id=%+v", root_page_id, real_page_id)
	}
	var entryCount int32
	_ = binary.Read(rootReader, binary.LittleEndian, &entryCount)
	log.Printf("root page has %+v entries\n", entryCount)

	var avaiable int32
	_, err = rootReader.Seek(4, io.SeekCurrent)
	if err != nil {
		log.Fatalf("Seek error %+v", err)
	}
	_ = binary.Read(rootReader, binary.LittleEndian, &avaiable)
	log.Printf("available space: %+v\n", avaiable)

	var flag byte
	_ = binary.Read(rootReader, binary.LittleEndian, &flag)
	if flag == 0 {
		log.Printf("root node is leaf node")
		_, err = rootReader.Seek(12, io.SeekCurrent)
		if err != nil {
			log.Fatalf("read entry seek error %+v", err)
		}

		for i := 0; i < int(entryCount); i++ {
			entry, err := ReadEntry(rootReader)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("entry value len: %+v", len(entry.Value))
		}
	} else {
		log.Printf("root node is index node")
	}

}
