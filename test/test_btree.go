package main

import (
	"bitbucket.org/santucco/btree"
	"encoding/binary"
	"flag"
	"log"
	"math/rand"
	"os"
)

var magic = [16]byte{'p', 'u', 'f', ',', ' ', 't', 'h', 'e', ' ', 'm', 'a', 'g', 'i', 'c', ' '}

const capacity = 1024
const FullCount = 195225786
const count = 10000000

type key struct {
	Id  uint64
	Off uint32
}

func (k key) Size() uint {
	return 12
}

func (k key) Compare(buf []byte) (int, error) {
	o := uint64(binary.LittleEndian.Uint64(buf[:8]))
	if k.Id < o {
		return -1, nil
	} else if k.Id > o {
		return 1, nil
	}
	return 0, nil
}

func (k *key) Read(buf []byte) error {
	k.Id = uint64(binary.LittleEndian.Uint64(buf[:8]))
	k.Off = uint32(binary.LittleEndian.Uint32(buf[:4]))
	return nil
}

func (k key) Write(buf []byte) error {
	binary.LittleEndian.PutUint64(buf[:8], k.Id)
	binary.LittleEndian.PutUint32(buf[8:], k.Off)
	return nil
}

func main() {
	flag.Parse()

	fn := flag.Arg(0)
	fh, err := os.Open(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatalf("cannot open %s: %s", fn, err)
		}
		log.Printf("file %s not exists yet", fn)
	}
	var tree btree.Tree
	if fh == nil {
		if fh, err = os.Create(fn); err != nil {
			log.Fatalf("cannot create %s: %s", fn, err)
		}
		defer fh.Close()

		if tree, err = btree.NewBTree(fh, magic, new(key), capacity); err != nil {
			log.Fatalf("error creating new tree: %s", err)
		}
		k := key{Id: uint64(rand.Int()), Off: 0}
		for i := 0; i < count; i++ {
			k.Id += uint64(1 + rand.Intn(1))
			k.Off = uint32(i)
			if _, err = tree.Insert(&k); err != nil {
				log.Fatalf("error inserting %s: %s", k, err)
			}
			if i%1000000 == 0 {
				log.Printf("%d", i)
			}
		}

	} else {
		if tree, err = btree.OpenBTree(fh, magic, new(key)); err != nil {
			log.Fatalf("error opening tree %s: %s", fn, err)
		}
		defer fh.Close()

		next := tree.Enum(new(key))
		i := 0
		for k, e := next(); k != nil && e == nil; k, e = next() {
			if i == 0 {
				log.Printf("first key is %s", k)
			}
			i++
		}
		log.Printf("key count is %d", i)
	}
}
