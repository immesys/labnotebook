package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	r "github.com/dancannon/gorethink"
)

var recordchan chan map[string]interface{}
var la *time.Location
var total int64

func mustEnv(key string) string {
	rv := os.Getenv(key)
	if rv == "" {
		fmt.Println("Missing $" + key)
		os.Exit(1)
	}
	return rv
}
func main() {
	var err error
	la, err = time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}
	recordchan = make(chan map[string]interface{}, 1000000)
	listenip := mustEnv("NB_LISTEN_ADDR")
	go procrecordchan()
	ln, err := net.Listen("tcp", listenip+":4050")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			time.Sleep(5 * time.Second)
			fmt.Println("total records inserted: ", atomic.LoadInt64(&total))
		}
	}()
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
			if err.Error() != "EOF" {
				fmt.Println("gob error: " + err.Error())
			}
			c.Close()
			return
		}
		newRecord(r)
	}
}

func newRecord(r map[string]interface{}) {
	atomic.AddInt64(&total, 1)
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
			rv[strings.ToLower(k)] = time.Unix(0, v.(int64)).In(la)
		} else {
			rv[strings.ToLower(k)] = v
		}
	}
	_, ok := rv["m"]
	if !ok {
		rv["m"] = "default"
	}
	_, ok = rv["sourcetime"]
	if !ok {
		rv["sourcetime"] = time.Now().In(la)
		rv["sourcetime_ok"] = false
	} else {
		rv["sourcetime_ok"] = true
	}
	rv["logtime"] = time.Now().In(la)
	return rv
}
func procrecordchan() {
	session, err := r.Connect(r.ConnectOpts{
		Address: mustEnv("NB_RETHINK"),
	})
	if err != nil {
		log.Fatalln(err)
	}
	metrix := mustEnv("NB_INFLUX")
	for i := 0; i < 10; i++ {
		go func() {
			lastM := time.Now()
			m_buffer := &bytes.Buffer{}
			for {
				docz := []map[string]interface{}{<-recordchan}
				for b := 0; b < 500; b++ {
					select {
					case doc := <-recordchan:
						docz = append(docz, doc)
					default:
						break
					}
				}
				for idx := range docz {
					docz[idx] = normalize(docz[idx])
				}
				_, err = r.DB("nb").Table("recs").Insert(docz).RunWrite(session)
				if err != nil {
					panic(err)
				}
				for _, doc := range docz {
					dots := false
					for k, _ := range doc {
						if strings.HasPrefix(k, "ts_") {
							dots = true
							break
						}
					}
					if dots {
						m_buffer.Write([]byte(doc["m"].(string)))
						for k, v := range doc {
							if strings.HasPrefix(k, "mt_") {
								m_buffer.Write([]byte{','})
								m_buffer.Write([]byte(k[3:]))
								m_buffer.Write([]byte{'='})
								m_buffer.Write([]byte(v.(string)))
							}
						}
						m_buffer.Write([]byte(",hostname="))
						m_buffer.Write([]byte(doc["hostname"].(string)))
						m_buffer.Write([]byte(",progname="))
						m_buffer.Write([]byte(doc["progname"].(string)))
						m_buffer.Write([]byte(" "))
						first := true
						for k, v := range doc {
							if !strings.HasPrefix(k, "ts_") {
								continue
							}
							vv, ok := v.(float64)
							if !ok {
								i, ok := v.(int64)
								if ok {
									vv = float64(i)
								} else {
									s := v.(string)
									vv, _ = strconv.ParseFloat(s, 64)
								}
							}
							if !first {
								m_buffer.Write([]byte{','})
							}
							first = false
							m_buffer.Write([]byte(k[3:]))
							m_buffer.Write([]byte{'='})
							m_buffer.Write([]byte(strconv.FormatFloat(vv, 'f', 6, 64)))

						}
						m_buffer.Write([]byte(" "))
						m_buffer.Write([]byte(strconv.FormatInt(doc["sourcetime"].(time.Time).UnixNano(), 10)))
						m_buffer.Write([]byte("\n"))
						if time.Now().After(lastM.Add(time.Second)) {
							resp, err := http.Post(fmt.Sprintf("http://%s/write?db=metrix", metrix), "text/plain", m_buffer)
							if err != nil {
								panic(err)
							}
							bd, _ := ioutil.ReadAll(resp.Body)
							if resp.StatusCode != 204 {
								fmt.Println("got code: ", resp.StatusCode)
								fmt.Println("body: ", string(bd))
							}
							resp.Body.Close()
							m_buffer.Reset()
							lastM = time.Now()
						}
					}
				} //end foreach doc
			} //end for ever
		}()
	}
	for {
		time.Sleep(10 * time.Second)
	}
}
