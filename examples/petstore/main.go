package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	echo "github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var (
	portFlag = flag.String("port", ":8081", "port to listen on")
	dbFlag   = flag.String("db", "petstore.db", "database file")
)

func main() {
	flag.Parse()

	db, err := NewDB(badger.DefaultOptions(*dbFlag))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	srv := NewServer(db)
	e := echo.New()
	e.Use(middleware.Logger())
	e.POST("/pet/:petId/uploadImage", srv.UploadImage)
	e.GET("/image/:id", srv.GetImage)
	e.POST("/pet", srv.AddPet)
	e.GET("/pet/:id", srv.GetPet)
	e.GET("/pet", srv.GetPets)
	e.DELETE("/pet/:id", srv.DeletePet)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// Start server
	go func() {
		if err := e.Start(*portFlag); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	// Wait for interrupt signal to gracefully shut down the server with a timeout of 10 seconds.
	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
}
