package manager

import (
	"fmt"
	"go-db/internal/buffer"
	"go-db/internal/catalog/schema"
	"go-db/internal/catalog/table"
	"go-db/internal/common/types"
	"go-db/internal/execution/executor"
	"go-db/internal/storage/disk"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type DB struct {
	executor *executor.Executor
}

func InitDatabase(dbBaseName string, bufferPoolSize int32) *DB {
	diskManager, err := disk.NewDiskStorage(dbBaseName)

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, bufferPoolSize)

	tablePageMap := make(map[string]types.Page_id_t)

	pageNumber := diskManager.GetPageNumber()

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

	tableManager := table.NewTableManager(bufferPool, tablePageMap)

	d := &DB{
		executor: executor.NewExecutor(bufferPool, diskManager, tableManager),
	}

	return d
}

func (d *DB) RunDB() {
	fmt.Println("Start Run go-DB")
	ginServer := gin.Default()

	ginServer.GET("/query", func(ctx *gin.Context) {
		response, err := d.executor.QueryExecutor(ctx.Query("query"))

		if err != nil {
			ctx.JSON(http.StatusBadRequest, err.Error())
		} else {
			ctx.JSON(http.StatusOK, string(response))
		}
	})

	ginServer.Run(":1234")
}
