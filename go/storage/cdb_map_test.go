package storage

import (
	"log"
	"math/rand"
	"os"
	"testing"
)

var testIndexFilename string = "../../test/sample.idx"

func TestCdbMap0Convert(t *testing.T) {
	indexFile, err := os.Open(testIndexFilename)
	if err != nil {
		t.Fatalf("cannot open %s: %s", testIndexFilename, err)
	}
	defer indexFile.Close()

	cdbFn := testIndexFilename + ".cdb"
	t.Logf("converting %s to %s", cdbFn, cdbFn)
	if err = ConvertIndexToCdb(cdbFn, indexFile); err != nil {
		t.Fatalf("error while converting: %s", err)
	}
}

func BenchmarkCdbMap1List(t *testing.B) {
    t.StopTimer()
	indexFile, err := os.Open(testIndexFilename)
	if err != nil {
		t.Fatalf("cannot open %s: %s", testIndexFilename, err)
	}
	defer indexFile.Close()
	t.Logf("opening %s", indexFile)
	idx, err := LoadNeedleMap(indexFile)
	if err != nil {
		t.Fatalf("cannot load %s: %s", indexFile, err)
	}
	defer idx.Close()

	cdbFn := testIndexFilename + ".cdb"
	t.Logf("opening %s", cdbFn)
	m, err := OpenCdbMap(cdbFn)
	if err != nil {
		t.Fatalf("error opening %s: %s", cdbFn, err)
	}
	defer m.Close()

	i := 0
	log.Printf("checking whether the cdb contains every key")
    t.StartTimer()
	err = idx.Visit(func(nv NeedleValue) error {
		if i > t.N || rand.Intn(10) < 9 {
			return nil
		}
		i++
		if i%1000 == 0 {
			log.Printf("%d. %s", i, nv)
		}
		if nv2, ok := m.Get(uint64(nv.Key)); !ok || nv2 == nil {
			t.Errorf("%s in index, not in cdb", nv.Key)
		} else if nv2.Key != nv.Key {
			t.Errorf("requested key %d from cdb, got %d", nv.Key, nv2.Key)
		} else if nv2.Offset != nv.Offset {
			t.Errorf("offset is %d in index, %d in cdb", nv.Offset, nv2.Offset)
		} else if nv2.Size != nv.Size {
			t.Errorf("size is %d in index, %d in cdb", nv.Size, nv2.Size)
		}
        t.SetBytes(int64(nv.Size))
		return nil
	})
    t.StopTimer()
	if err != nil {
		t.Errorf("error visiting index: %s", err)
	}

	i = 0
	log.Printf("checking wheter the cdb contains no stray keys")
    t.StartTimer()
	err = m.Visit(func(nv NeedleValue) error {
		if i > t.N || rand.Intn(10) < 9 {
			return nil
		}
		if nv2, ok := m.Get(uint64(nv.Key)); !ok || nv2 == nil {
			t.Errorf("%s in cdb, not in index", nv.Key)
		} else if nv2.Key != nv.Key {
			t.Errorf("requested key %d from index, got %d", nv.Key, nv2.Key)
		} else if nv2.Offset != nv.Offset {
			t.Errorf("offset is %d in cdb, %d in index", nv.Offset, nv2.Offset)
		} else if nv2.Size != nv.Size {
			t.Errorf("size is %d in cdb, %d in index", nv.Size, nv2.Size)
		}
		i++
		if i%1000 == 0 {
			log.Printf("%d. %s", i, nv)
		}
        t.SetBytes(int64(nv.Size))
		return nil
	})
    t.StopTimer()
	if err != nil {
		t.Errorf("error visiting index: %s", err)
	}
}
