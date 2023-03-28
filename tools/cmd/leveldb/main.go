package main

import (
	"fmt"
	"log"

	"flag"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	var dest string
	var pairs int
	flag.StringVar(&dest, "d", "demo", "")
	flag.IntVar(&pairs, "p", 10, "")

	flag.Parse()
	log.Println("db path: ", dest)

	o := &opt.Options{}
	db, err := leveldb.OpenFile(dest, o)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < pairs; i++ {
		k := fmt.Sprintf("key_%d", i)
		v := fmt.Sprintf("value_%d", i)

		if err := db.Put([]byte(k), []byte(v), nil); err != nil {
			log.Fatal(err)
		}
	}
}
