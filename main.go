package main

import (
	"andb/core"
	"andb/db"
	"fmt"
	"os"
)

func main() {
	db, err := db.OpenDB("./testdbfolder", &db.DBOptions{CreateIfMissing: true})
	if err != nil {
		fmt.Println("Could not open db: ", err)
		os.Exit(1)
	}

	key := core.Key("hello")

	db.Put(key, []byte("world!"))
	data, err := db.Get("hello")

	if err != nil {
		fmt.Println("Could not get key: ", err)
		os.Exit(1)
	}

	fmt.Printf("%v: %v\n", key, data)
}
