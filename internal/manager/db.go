package manager

import (
	"fmt"
	"go-db/internal/buffer"
	"go-db/internal/catalog/schema"
	"go-db/internal/catalog/table"
	"go-db/internal/common/types"
	"go-db/internal/storage/disk"
	"log"
	"time"
)

type DB struct {
	bufferPool   *buffer.BufferPoolManager
	diskManager  *disk.Disk
	tableManager *table.TableManager
}

func InitDatabase(dbBaseName string, bufferPoolSize int32) *DB {
	diskManager, err := disk.NewDiskStorage(dbBaseName)

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, bufferPoolSize)

	d := &DB{
		diskManager: diskManager,
		bufferPool:  bufferPool,
	}

	tablePageMap := make(map[string]types.Page_id_t)

	pageNumber := d.diskManager.GetPageNumber()

	for i := 0; i < int(pageNumber); i++ {
		diskPage, err := bufferPool.FetchPage(types.Page_id_t(i))

		if err != nil {
			log.Fatal(err)
		}

		if diskPage.GetPageTye() == types.META_PAGE_TYPE {
			metaPage := schema.GetSchema(diskPage)
			tablePageMap[metaPage.GetTableName()] = types.Page_id_t(i)
		}
	}

	d.tableManager = table.NewTableManager(bufferPool, tablePageMap)

	return d
}

func (d *DB) RunDB() {
	fmt.Println("Start Run go-DB")
	for {
		time.Sleep(10 * time.Second)
	}
}
