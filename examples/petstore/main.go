package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/codec/lex"
	"github.com/ehsanranjbar/badgerutils/examples/petstore/types"
	"github.com/ehsanranjbar/badgerutils/iters"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	echo "github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var (
	db             *badger.DB
	petsRepository = sstore.New[types.Pet](pstore.New(nil, []byte("pets")))
)

func main() {
	var err error
	db, err = badger.Open(badger.DefaultOptions("").WithInMemory(true))
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
	var p types.Pet
	if err := c.Bind(&p); err != nil {
		return err
	}

	err := db.Update(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		return repo.Set(lex.EncodeInt64(p.Id), &p)
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to create pet: %w", err))
	}

	return c.JSON(200, &p)
}

func getPet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	var p *types.Pet
	err = db.View(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		p, err = repo.Get(lex.EncodeInt64(id))
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

	var pets []*types.Pet
	err = db.View(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		it := iters.Limit(
			iters.SkipN(
				repo.NewIterator(badger.DefaultIteratorOptions),
				int(offset),
			),
			int(limit),
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
