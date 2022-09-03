package disk

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

	fi, err := file.Stat()

	if err != nil {
		return nil, err
	}

	d := &Disk{
		fileName:   DBFileName,
		nextPageID: types.Page_id_t(fi.Size() / int64(constant.PAGE_SIZE)),
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

func (D *Disk) WritePageOffset(pageID types.Page_id_t, offset uint32, pageData []byte) error {
	pageOffset := pageID * constant.PAGE_SIZE

	_, err := D.file.Seek(int64(pageOffset)+int64(offset), 0)

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

func (D *Disk) GetPageNumber() int32 {
	stat, _ := D.file.Stat()
	fileSize := stat.Size()
	return int32(fileSize) / constant.PAGE_SIZE
}

func (D *Disk) AllocatePage() types.Page_id_t {
	pageID := D.nextPageID
	D.nextPageID++
	return pageID
}
