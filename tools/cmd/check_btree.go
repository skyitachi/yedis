package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
)

const (
	// PAGE_SIZE            = 4096
	PAGE_SIZE            = 128
	MetaPageIdOffset     = 0
	MetaRootPageIdOffset = 5
	MetaLevelOffset      = 9

	FlagOffset       = 4
	EntryCountOffset = 5
	AvailableOffset  = 13
	EntryOffset      = 29

	DegreeOffset  = 9
	KeyPostOffset = 21
)

func main() {
	idxFile := "btree.idx"
	if len(os.Args) > 1 {
		idxFile = os.Args[len(os.Args)-1]
	}
	log.Printf("index file: %s", idxFile)
	btreeFile, err := os.Open(idxFile)
	if err != nil {
		log.Fatal(err)
	}
	fileInfo, err := os.Stat(idxFile)
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i < int(fileInfo.Size())/PAGE_SIZE; i++ {
		ReadPage(int32(i), btreeFile)
	}

}

func ReadPage(page_id int32, btreeFile *os.File) {
	buf := make([]byte, PAGE_SIZE)
	_, err := btreeFile.ReadAt(buf, int64(page_id*PAGE_SIZE))
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
	if real_page_id != page_id {
		log.Fatalf("meta root_page_id=%+v, real_page_id=%+v", page_id, real_page_id)
	}
	// parse flag
	var flag byte
	_ = binary.Read(rootReader, binary.LittleEndian, &flag)

	var entryCount int32
	_ = binary.Read(rootReader, binary.LittleEndian, &entryCount)
	log.Printf("[page_id %v] root page has %+v entries\n", page_id, entryCount)

	if flag == 0 {
		// leaf page
		var avaiable int32
		_, err = rootReader.Seek(4, io.SeekCurrent)
		if err != nil {
			log.Fatalf("Seek error %+v", err)
		}
		_ = binary.Read(rootReader, binary.LittleEndian, &avaiable)
		_, err = rootReader.Seek(12, io.SeekCurrent)
		if err != nil {
			log.Fatalf("read entry seek error %+v", err)
		}
		log.Printf("[leaf] [page_id %+v] available=%+v, entryCount=%+v", real_page_id, avaiable, entryCount)
		return
	}
	// index node
	var degree int32
	_ = binary.Read(rootReader, binary.LittleEndian, &degree)
	// skip
	_, err = rootReader.Seek(KeyPostOffset-AvailableOffset, io.SeekCurrent)
	if err != nil {
		log.Fatalf("index node skip error %+v", err)
	}

	keys, childs, err := ReadIndexNode(rootReader, int(entryCount), int(degree))
	log.Printf("[index] [page_id %+v] degree=%+v, keys=%+v, childs=%+v", real_page_id, degree, keys, childs)

	if err != nil {
		log.Fatalf("read index node failed: %+v", err)
	}
}

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
	_, err = reader.Seek(int64(KeyPostOffset+(2*degree-1)*8), io.SeekStart)
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
