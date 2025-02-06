package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/codec/lex"
	"github.com/ehsanranjbar/badgerutils/iters"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	echo "github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

type Pet struct {
	Id        int64    `json:"id"`
	Category  Category `json:"category"`
	Name      string   `json:"name"`
	PhotoUrls []string `json:"photoUrls"`
	Tags      []Tag    `json:"tags"`
	Status    string   `json:"status"`
}

type MsgpackWrapper[T any] struct {
	wrapped T
}

func (w MsgpackWrapper[T]) MarshalBinary() (data []byte, err error) {
	return msgpack.Marshal(w.wrapped)
}

func (w *MsgpackWrapper[T]) UnmarshalBinary(data []byte) error {
	return msgpack.Unmarshal(data, &w.wrapped)
}

type Category struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type Tag struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

var (
	db             *badger.DB
	petsRepository = sstore.New[MsgpackWrapper[Pet]](pstore.New(nil, []byte("pets")))
)

func main() {
	var err error
	db, err = badger.Open(badger.DefaultOptions("tmp/petstore.db"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	e := echo.New()
	e.Use(middleware.Logger())
	e.POST("/pet", addPet)
	e.GET("/pet/:id", getPet)
	e.GET("/pet", getPets)
	e.DELETE("/pet/:id", deletePet)

	e.Logger.Fatal(e.Start(":8081"))
}

func addPet(c echo.Context) error {
	var p Pet
	if err := c.Bind(&p); err != nil {
		return err
	}

	err := db.Update(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		return repo.Set(lex.EncodeInt64(p.Id), &MsgpackWrapper[Pet]{p})
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to create pet: %w", err))
	}

	return c.JSON(200, p)
}

func getPet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	var p Pet
	err = db.View(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		wp, err := repo.Get(lex.EncodeInt64(id))
		if wp != nil {
			p = wp.wrapped
		}
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Errorf("pet not found"))
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get pet: %w", err))
	}

	return c.JSON(200, p)
}

func getPets(c echo.Context) error {
	offset, err := strconv.ParseInt(c.QueryParam("offset"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse offset: %w", err))
	}
	limit, err := strconv.ParseInt(c.QueryParam("limit"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse limit: %w", err))
	}

	var pets []Pet
	err = db.View(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		it := iters.Map(
			iters.Limit(
				iters.SkipN(
					repo.NewIterator(badger.DefaultIteratorOptions),
					int(offset),
				),
				int(limit),
			),
			func(v *MsgpackWrapper[Pet], _ *badger.Item) (Pet, error) {
				return v.wrapped, nil
			},
		)
		defer it.Close()
		pets, err = iters.Collect(it)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to get pets: %w", err)
	}

	return c.JSON(http.StatusOK, pets)
}

func deletePet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	err = db.Update(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		return repo.Delete(lex.EncodeInt64(id))
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to delete pet: %w", err))
	}

	return c.NoContent(http.StatusNoContent)
}
