package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/examples/petstore/types"
	"github.com/ehsanranjbar/badgerutils/iters"
	objstore "github.com/ehsanranjbar/badgerutils/store/object"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	echo "github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var (
	db             *badger.DB
	petsRepository *objstore.Store[int64, types.Pet]
)

func main() {
	var err error
	db, err = badger.Open(badger.DefaultOptions("tmp/petstore.db"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	petsRepository, err = objstore.New[int64, types.Pet](pstore.New(nil, []byte("pets")))
	if err != nil {
		panic(err)
	}

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

	var obj *objstore.Object[int64, types.Pet]
	err := db.Update(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		err := repo.Set(p, objstore.WithId[int64, types.Pet](p.Id))
		if err != nil {
			return err
		}
		obj, err = repo.GetObject(p.Id)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to create pet: %w", err))
	}

	return c.JSON(http.StatusCreated, obj)
}

func getPet(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("failed to parse id: %w", err))
	}

	var obj *objstore.Object[int64, types.Pet]
	err = db.View(func(txn *badger.Txn) error {
		repo := petsRepository.Instantiate(txn)
		obj, err = repo.GetObject(id)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Errorf("pet not found"))
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to get pet: %w", err))
	}

	return c.JSON(http.StatusOK, obj)
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
		it := iters.Map(
			iters.Limit(
				iters.SkipN(
					repo.NewIterator(badger.DefaultIteratorOptions),
					int(offset),
				),
				int(limit),
			),
			func(obj *objstore.Object[int64, types.Pet], _ *badger.Item) (*types.Pet, error) {
				return &obj.Data, nil
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
		return repo.Delete(id)
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("failed to delete pet: %w", err))
	}

	return c.NoContent(http.StatusNoContent)
}
