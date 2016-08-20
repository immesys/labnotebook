package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	r "github.com/dancannon/gorethink"
)

var recordchan chan map[string]interface{}

func mustEnv(key string) string {
	rv := os.Getenv(key)
	if rv == "" {
		fmt.Println("Missing $" + key)
		os.Exit(1)
	}
	return rv
}
func main() {
	recordchan = make(chan map[string]interface{}, 10000)
	listenip := mustEnv("NB_LISTEN_ADDR")
	go procrecordchan()
	ln, err := net.Listen("tcp", listenip+":4050")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(c net.Conn) {
	enc := gob.NewDecoder(c)
	for {
		var r map[string]interface{}
		err := enc.Decode(&r)
		if err != nil {
			fmt.Println("gob error: " + err.Error())
			c.Close()
			return
		} else {
			fmt.Println("got: ", r)
		}
		newRecord(r)
		fmt.Println("inserted record")
	}
}

func newRecord(r map[string]interface{}) {
	select {
	case recordchan <- r:
	default:
		panic("Ran out of space in channel")
	}
}
func normalize(r map[string]interface{}) map[string]interface{} {
	rv := make(map[string]interface{})
	for k, v := range r {
		if strings.ToLower(k) == "sourcetime" {
			rv[strings.ToLower(k)] = time.Unix(0, v.(int64))
		} else {
			rv[strings.ToLower(k)] = v
		}
	}
	return rv
}
func procrecordchan() {
	session, err := r.Connect(r.ConnectOpts{
		Address: mustEnv("NB_RETHINK"),
	})
	if err != nil {
		log.Fatalln(err)
	}
	for i := 0; i < 10; i++ {
		go func() {
			for {
				doc := <-recordchan
				doc = normalize(doc)
				_, ok := doc["m"]
				if !ok {
					doc["m"] = "default"
				}
				_, ok = doc["sourcetime"]
				if !ok {
					doc["sourcetime"] = time.Now()
					doc["sourcetime_ok"] = false
				} else {
					doc["sourcetime_ok"] = true
				}
				doc["logtime"] = time.Now()
				_, err = r.DB("nb").Table("recs").Insert(doc).RunWrite(session)
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	for {
		time.Sleep(10 * time.Second)
	}
}
