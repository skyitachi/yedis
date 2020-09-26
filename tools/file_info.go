package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

const (
	PAGE_SIZE            = 4096
	MetaPageIdOffset     = 0
	MetaRootPageIdOffset = 4
	MetaLevelOffset      = 8
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

}
