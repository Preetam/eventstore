package eventstore

import (
	"errors"
	"strconv"
	"sync"

	"github.com/Preetam/lm2"
)

var (
	errNotFound = errors.New("not found")
)

// Event represents a flat JSON event object.
type Event map[string]interface{}

// EventCollection is a collection of events.
type EventCollection struct {
	lm2Col *lm2.Collection
	lock   sync.Mutex
}

// OpenEventCollection opens an event collection located at path.
func OpenEventCollection(path string) (*EventCollection, error) {
	col, err := lm2.OpenCollection(path, 10000)
	if err != nil {
		return nil, err
	}

	return &EventCollection{
		lm2Col: col,
	}, nil
}

// CreateEventCollection creates an event collection located at path.
func CreateEventCollection(path string) (*EventCollection, error) {
	col, err := lm2.NewCollection(path, 10000)
	if err != nil {
		return nil, err
	}

	return &EventCollection{
		lm2Col: col,
	}, nil
}

// Version returns the version of the collection.
func (c *EventCollection) Version(tag string) (int, error) {
	cur, err := c.lm2Col.NewCursor()
	if err != nil {
		return 0, err
	}

	versionStr, err := cursorGet(cur, string(versionKeyPrefix)+"-"+tag)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(versionStr)
}
