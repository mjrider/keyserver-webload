package main

import (
	"bufio"
	"bytes"
	"flag"
	"net/http"
	"net/url"
	"os"

	"fmt"
	"gopkg.in/errgo.v1"
	"gopkg.in/hockeypuck/openpgp.v1"
	"log"
)

var (
	endpoint = flag.String("endpoint", "http://127.0.0.1:11371", "server where the keyserver lives")
	workers  = flag.Int("workers", 5, "Workers for shipping keys")
)

type KeyChan chan []*openpgp.PrimaryKey

// reads pgp file stream into chan
func readFile(file string, c KeyChan) {
	f, err := os.Open(file)
	if err != nil {
		log.Printf("Could not open %q: %s", file, err)

	} else {
		defer f.Close()
		var keys []*openpgp.PrimaryKey
		keys = make([]*openpgp.PrimaryKey, 0, 100)

		for kr := range openpgp.ReadKeys(f) {
			if kr.Error != nil {
				log.Printf("Key could not be read: %v", errgo.Details(kr.Error))
			} else {
				keys = append(keys, kr.PrimaryKey)
				if len(keys) >= 100 {
					c <- keys
					keys = make([]*openpgp.PrimaryKey, 0, 100)
				}
			}
		}
		if len(keys) >= 1 {
			c <- keys
		}
	}
}

// load specified files
func loadFiles(files []string, c KeyChan) {
	for _, file := range files {
		log.Printf("Processing file: %s\n", file)
		readFile(file, c)
	}
	// all files handled, close channel
	close(c)
}

func shipit(w int, keys []*openpgp.PrimaryKey) {
	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	err := openpgp.WriteArmoredPackets(writer, keys)
	keytext := b.String()

	if err != nil {
		log.Printf("Worker: %d: Armor error %s \n", w, err)
	} else {
		if len(keytext) > 0 {
			resp, err := http.PostForm(*endpoint+"/pks/add", url.Values{
				"keytext": []string{keytext},
			})
			if err != nil {
				log.Printf("Worker: %d, error: %s", w, err)
			}
			if resp != nil {
				resp.Body.Close()
			}
		} else {
			log.Printf("Worker: %d: key resulted in empty bufffer %#v\n", w)
		}
	}
}

func shipper(w int, c KeyChan) {
	for keys := range c {
		shipit(w, keys)
	}
}

func load(args []string) {
	// key channel
	kc := make(KeyChan, 100)
	for w := 1; w <= *workers; w++ {
		go shipper(w, kc)
	}
	loadFiles(args, kc)
}

func main() {

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		fmt.Printf("usage: %s [flags] <file1> [file2 .. fileN]", os.Args[0])
		flag.PrintDefaults()
	}

	load(args)
}
