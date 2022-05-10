package storage

import (
	"os"
	"testing"
)

func Test_Simple_Disk(t *testing.T) {
	disk, err := NewDiskStorage("test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test.db")

	pageID := disk.AllocatePage()

	err = disk.WritePage(pageID, []byte("12345"))

	if err != nil {
		t.Fatal(err)
	}

	data, err := disk.ReadPage(pageID)
	if err != nil {
		t.Fatal(err)
	}

	if data[0] != '1' {
		t.Fatal("Wrong", string(data))
	}
	defer os.Remove("test.db")
}

func Test_PageIDOffSet_Disk(t *testing.T) {
	disk, err := NewDiskStorage("test.db")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove("test.db")

	pageID := disk.AllocatePage()

	err = disk.WritePage(pageID, []byte("12345"))

	if err != nil {
		t.Fatal(err)
	}

	nextPageID := disk.AllocatePage()

	err = disk.WritePage(nextPageID, []byte("45678"))

	if err != nil {
		t.Fatal(err)
	}

	data, err := disk.ReadPage(pageID)
	if err != nil {
		t.Fatal(err)
	}

	nextdata, err := disk.ReadPage(nextPageID)
	if err != nil {
		t.Fatal(err)
	}

	if data[0] != '1' {
		t.Fatal("Wrong", string(data))
	}

	if nextdata[0] != '4' {
		t.Fatal("Wrong", string(nextdata))
	}
}
