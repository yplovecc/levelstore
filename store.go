package main

import (
	"bufio"
	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"net/http"
	"os"
	"strings"
)

type LevelStore struct {
	db *leveldb.DB
}

type Item struct {
	k string
	v string
}

func (s *LevelStore) Open(p string) (err error) {
	s.db, err = leveldb.OpenFile(p, nil)
	return err
}

func (s *LevelStore) Close() {
	s.db.Close()
}

func (s *LevelStore) BatchWFromFileHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	p := r.FormValue("path")
	filepaths := getFilelist(p)
	glog.Infof("find %d files", len(filepaths))
	var items []Item
	count := 0
	v := "write ok"
	for i, filepath := range filepaths {
		glog.Infof("read item %d : %s", i, filepath)
		f, err := os.Open(filepath)
		defer f.Close()
		if err != nil {
			glog.Error(err)
			v = "open " + filepath + " failed"
			break
		}
		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				glog.Info(err)
				break
			}
			temp := strings.Split(string(line), "\t")
			if len(temp) != 2 {
				continue
			}
			i := Item{k: temp[0], v: temp[1]}
			items = append(items, i)
			count = count + 1
			if count%(*batchsize) == 0 {
				s.BatchWrite(items)
				count = 0
				items = items[:0]
			}
		}
		s.BatchWrite(items)
		count = 0
		items = items[:0]
		glog.Infof("write ok %s", filepath)
		f.Close()
	}
	mustEncode(w, struct {
		Value string `json:"value"`
	}{Value: v})
}

func (s *LevelStore) BatchWrite(data []Item) {
	batch := new(leveldb.Batch)
	defer batch.Reset()
	for _, i := range data {
		batch.Put([]byte(i.k), []byte(i.v))
	}
	op := opt.WriteOptions{NoWriteMerge: true}
	err := s.db.Write(batch, &op)
	if err != nil {
		glog.Error(err)
	}
	glog.Infof("batch write %d items", len(data))
}

func (s *LevelStore) GetHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	n := r.FormValue("n")
	v, err := s.db.Get([]byte(n), nil)
	if err != nil {
		v = []byte("not found")
	}
	glog.Info(string(v))
	//mustEncode(w, raw)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-type", "application/json;charset=utf-8")
	w.Write(v)
}
