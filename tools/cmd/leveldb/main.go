package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
	"log"
	"os"
)

func main() {
	//var dest string
	//var pairs int
	//flag.StringVar(&dest, "d", "demo", "")
	//flag.IntVar(&pairs, "p", 10, "")
	//
	//flag.Parse()
	//log.Println("db path: ", dest)
	//
	o := &opt.Options{}
	o.Filter = filter.NewBloomFilter(8)
	fmt.Println(o.Filter.Name())
	o.Compression = opt.NoCompression
	//
	//wr, err := os.OpenFile("000001.gldb", os.O_RDWR|os.O_CREATE, 0644)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//tableBuilder := table.NewWriter(wr, o)
	//
	//kvs := [][]string{
	//	{"a", "v1"}, {"ab", "abc"}, {"abcd", "abcde"}, {"abcde", "ab"},
	//}
	//
	//for _, kv := range kvs {
	//	if err := tableBuilder.Append([]byte(kv[0]), []byte(kv[1])); err != nil {
	//		log.Fatal(err)
	//	}
	//}
	//
	//tableBuilder.Close()

	reader, err := os.OpenFile("000001.ldb", os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	fileInfo, err := reader.Stat()
	if err != nil {
		log.Fatal(err)
	}
	fileDesc := storage.FileDesc{
		Type: storage.TypeTable,
		Num:  int64(reader.Fd()),
	}
	tableReader, err := table.NewReader(reader, fileInfo.Size(), fileDesc, nil, nil, o)
	if err != nil {
		log.Fatal(err)
	}

	readOptions := &opt.ReadOptions{}
	iter := tableReader.NewIterator(nil, readOptions)

	for iter.Valid() {
		iter.Key()
	}

	//db, err := leveldb.OpenFile(dest, o)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//for i := 0; i < pairs; i++ {
	//	k := fmt.Sprintf("key_%d", i)
	//	v := fmt.Sprintf("value_%d", i)
	//
	//	if err := db.Put([]byte(k), []byte(v), nil); err != nil {
	//		log.Fatal(err)
	//	}
	//}
}
