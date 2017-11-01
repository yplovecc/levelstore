package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
)

func mustEncode(w http.ResponseWriter, i interface{}) {
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-type", "application/json;charset=utf-8")
	e := json.NewEncoder(w)
	if err := e.Encode(i); err != nil {
		//panic(err)
		e.Encode(err.Error())
	}
}

func errorMessage(w http.ResponseWriter, err error) {
	if err != nil {
		mustEncode(w, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{Status: "error", Message: err.Error()})
	}
}

func getFilelist(path string) (files []string) {
	filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		//println(path)
		files = append(files, path)
		return nil
	})
	return
}
