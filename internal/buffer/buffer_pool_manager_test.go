package buffer

import (
	"bytes"
	"go-db/internal/common/constant"
	"go-db/internal/common/types"
	"go-db/internal/storage/disk"
	"math/rand"
	"testing"
)

func Test_BufferPoolManager_Simple(t *testing.T) {
	lruReplacer := NewLRUReplacer()
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		t.Fatal(err)
	}

	bufferpool := NewBufferPoolManager(lruReplacer, diskManager, 5)
	pages := make([]types.Page_id_t, 0, 5)
	randomData := make([][]byte, 5)

	for i := range randomData {
		randomData[i] = make([]byte, constant.PAGE_SIZE)
	}

	for i := 0; i < 5; i++ {
		rand.Read(randomData[i])

		page, err := bufferpool.NewPage()

		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, page.GetPageID())
		copy(page.GetData(), randomData[i])
		if !bytes.Equal(page.GetData(), randomData[i]) {
			t.Fatal("wrong data")
		}
	}

	for i := 0; i < 5; i++ {
		bufferpool.UnpinPage(pages[i])
	}

	page, err := bufferpool.FetchPage(0)

	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(page.GetData(), randomData[0]) {
		t.Fatal("wrong data")
	}
}

func Test_BufferPoolManager_full(t *testing.T) {
	lruReplacer := NewLRUReplacer()
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		t.Fatal(err)
	}

	bufferpool := NewBufferPoolManager(lruReplacer, diskManager, 5)

	randomData := make([][]byte, 5)

	for i := range randomData {
		randomData[i] = make([]byte, constant.PAGE_SIZE)
	}

	for i := 0; i < 5; i++ {
		rand.Read(randomData[i])

		page, err := bufferpool.NewPage()

		if err != nil {
			t.Fatal(err)
		}
		copy(page.GetData(), randomData[i])

		if !bytes.Equal(page.GetData(), randomData[i]) {
			t.Fatal("wrong data")
		}
		bufferpool.FlushPage(page.GetPageID())
	}

	bufferpool.UnpinPage(0)

	fullPage, err := bufferpool.NewPage()
	newData := make([]byte, constant.PAGE_SIZE)
	rand.Read(newData)

	if err != nil {
		t.Fatal(err)
	}

	copy(fullPage.GetData(), newData)

	if !bytes.Equal(fullPage.GetData(), newData) {
		t.Fatal("wrong data")
	}

	bufferpool.UnpinPage(1)

	oldPage, err := bufferpool.FetchPage(0)

	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(oldPage.GetData(), randomData[0]) {
		t.Fatal("wrong data")
	}
}
