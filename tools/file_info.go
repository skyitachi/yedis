package main

import (
	"bytes"
	"encoding/binary"
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
)

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
	skipped, err := rootReader.Seek(4, io.SeekCurrent)
	if err != nil {
		log.Fatalf("Seek error %+v", err)
	}
	log.Println("current offset: ", skipped)
	_ = binary.Read(rootReader, binary.LittleEndian, &avaiable)
	log.Printf("available space: %+v\n", avaiable)

	var flag byte
	_ = binary.Read(rootReader, binary.LittleEndian, &flag)
	if flag == 0 {
		log.Printf("root node is leaf node")
	} else {
		log.Printf("root node is index node")
	}

}