package main

import (
	"flag"
	"github.com/golang/glog"
	"net/http"
	"sync"
)

var (
	dbPath     = flag.String("dbpath", "db", "path to db")
	batchsize  = flag.Int("batchsize", 2000000, "batch write size")
	serverAddr = flag.String("addr", ":8080", "bind address")
)

type store interface {
	GetHandler(w http.ResponseWriter, r *http.Request)
	BatchWFromFileHandler(w http.ResponseWriter, r *http.Request)
	Open(p string) error
	Close()
}

func main() {
	flag.Parse()
	glog.Info("server start")
	defer glog.Info("server exit")
	var st store = &LevelStore{}
	err := st.Open(*dbPath)
	if err != nil {
		glog.Error("open db fail, %s", err)
		return
	}
	defer st.Close()

	http.HandleFunc("/n", st.GetHandler)
	http.HandleFunc("/w", st.BatchWFromFileHandler)

	var wg sync.WaitGroup
	wg.Add(1)
	go http.ListenAndServe(*serverAddr, nil)
	wg.Wait()
}
