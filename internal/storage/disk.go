package storage

import (
	"go-db/internal/common/constant"
	"go-db/internal/common/types"
	"os"
)

type Disk struct {
	fileName   string
	nextPageID types.Page_id_t
	file       *os.File
}

func NewDiskStorage(DBFileName string) (*Disk, error) {

	file, err := os.OpenFile(DBFileName, os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		return nil, err
	}

	d := &Disk{
		fileName:   DBFileName,
		nextPageID: 0,
		file:       file,
	}

	return d, nil
}

func (D *Disk) ShutDown() {
	D.file.Close()
}

func (D *Disk) WritePage(pageID types.Page_id_t, pageData []byte) error {
	offset := pageID * constant.PAGE_SIZE

	_, err := D.file.Seek(int64(offset), 0)

	if err != nil {
		return err
	}

	_, err = D.file.Write(pageData)

	if err != nil {
		return err
	}

	D.file.Sync()
	return nil
}

func (D *Disk) ReadPage(pageID types.Page_id_t) (data []byte, err error) {
	data = make([]byte, constant.PAGE_SIZE)
	offset := pageID * constant.PAGE_SIZE

	_, err = D.file.Seek(int64(offset), 0)

	if err != nil {
		return nil, err
	}

	D.file.Read(data)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (D *Disk) AllocatePage() types.Page_id_t {
	pageID := D.nextPageID
	D.nextPageID++
	return pageID
}
