package main

import (
	"encoding/binary"
	"flag"
	"github.com/tgulacsi/go-cdb"
	"log"
	"math/rand"
	"os"
)

const capacity = 1024
const FullCount = 195225786
const count = 10000000

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

	if fh == nil {
		if fh, err = os.Create(fn); err != nil {
			log.Fatalf("cannot create %s: %s", fn, err)
		}
		defer fh.Close()

		adder, closer, err := cdb.MakeFactory(fh)
		if err != nil {
			log.Fatalf("cannot create adder: %s", err)
		}
		defer closer()

		k := uint64(rand.Int())
		elt := cdb.Element{Key: make([]byte, 8), Data: make([]byte, 4)}
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint64(elt.Key, k)
			binary.LittleEndian.PutUint32(elt.Data, uint32(i))
			k += uint64(1 + rand.Intn(1))
			if err = adder(elt); err != nil {
				log.Fatalf("error inserting %s: %s", elt, err)
			}
			if i%1000000 == 0 {
				log.Printf("%d", i)
			}
		}

	} else {
		defer fh.Close()
		i := 0
		if err = cdb.DumpMap(fh, func(elt cdb.Element) error {
			if i == 0 {
				log.Printf("first element is %d=%d",
					binary.LittleEndian.Uint64(elt.Key),
					binary.LittleEndian.Uint32(elt.Data))
			}
			i++
			return nil
		}); err != nil {
			log.Fatalf("error dumping %s: %s", fh, err)
		}
		log.Printf("dumped %d elements", i)
	}
}
