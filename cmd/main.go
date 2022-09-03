package main

import "go-db/internal/manager"

const DEFAULT_BUFFER_POOL_SIZE = 2048

func main() {
	// use the http server as host
	// to receive the client SQL request
	db := manager.InitDatabase("test.db", DEFAULT_BUFFER_POOL_SIZE)
	db.RunDB()
}
