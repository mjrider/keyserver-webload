package main

import (
	"bufio"
	"bytes"
	"flag"
	"net/http"
	"net/url"
	"os"

	"gopkg.in/errgo.v1"
	log "gopkg.in/hockeypuck/logrus.v0"
	"gopkg.in/hockeypuck/openpgp.v1"
)

var (
	endpoint = flag.String("endpoint", "http://127.0.0.1:11371", "server where the keyserver lives")
	workers  = flag.Int("workers", 50, "Workers for shipping keys")
)

func main() {

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		log.Error("usage: %s [flags] <file1> [file2 .. fileN]", os.Args[0])
	}

	load(args)
}

// reads pgp file stream into chan
func readFile(file string, c chan<- *openpgp.PrimaryKey) {
	f, err := os.Open(file)
	if err != nil {
		log.Errorf("Could not open %q: %s", file, err)

	} else {
		defer f.Close()
		for kr := range openpgp.ReadKeys(f) {
			if kr.Error != nil {
				log.Errorf("Key could not be read: %v", errgo.Details(kr.Error))
			} else {
				c <- kr.PrimaryKey
			}
		}
	}
}

// load specified files
func loadFiles(files []string, c chan<- *openpgp.PrimaryKey) {
	for _, file := range files {
		readFile(file, c)
	}
	// all files handled, close channel
	close(c)
}

func shipit(w int, key *openpgp.PrimaryKey) {
	var b bytes.Buffer
	writer := bufio.NewWriter(&b)

	err := openpgp.WriteArmoredPackets(writer, []*openpgp.PrimaryKey{key})
	keytext := b.String()

	if err != nil {
		log.Errorf("Worker: %d: Armor error %s \n", w, err)
	} else {
		if len(keytext) > 0 {
			resp, err := http.PostForm(*endpoint+"/pks/add", url.Values{
				"keytext": []string{keytext},
			})
			if err != nil {
				log.Errorf("Worker: %d, error: %s, resp: %v", w, err, resp)
			}
			resp.Body.Close()
		} else {
			log.Errorf("Worker: %d: key resulted in empty bufffer\n", w)
		}
	}
}

func shipper(w int, c <-chan *openpgp.PrimaryKey) {
	for key := range c {
		lkey := key
		shipit(w, lkey)
	}
}

func load(args []string) error {
	// key channel
	kc := make(chan *openpgp.PrimaryKey, 100)
	for w := 1; w <= *workers; w++ {
		go shipper(w, kc)
	}
	loadFiles(args, kc)
	return nil
}
