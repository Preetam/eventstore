package eventstore

import (
	"encoding/json"
	"errors"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/Preetam/lm2"
)

var (
	eventIDTagRegexp  = regexp.MustCompile("^[a-zA-Z0-9_./]{1,256}$")
	eventIDHashRegexp = regexp.MustCompile("^[a-zA-Z0-9]{1,16}$")

	minTimestamp = time.Unix(0, 0)
)

type CreateEventsRequest struct {
	Tag     string  `json:"tag"`
	Version int     `json:"version"`
	Events  []Event `json:"events"`
}

func (c *EventCollection) StoreEvents(req CreateEventsRequest) (int, error) {
	// Validate tag
	if !eventIDTagRegexp.MatchString(req.Tag) {
		log.Println(req.Tag)
		return 0, errors.New("invalid tag")
	}

	events := req.Events

	wb := lm2.NewWriteBatch()
	for _, event := range events {
		delete(event, "_id")

		marshalled, err := json.Marshal(event)
		if err != nil {
			return 0, err
		}

		var ts int64
		if tsVal, ok := event["_ts"]; ok {
			if tsString, ok := tsVal.(string); ok {
				timeTs, err := time.Parse(time.RFC3339Nano, tsString)
				if err != nil {
					return 0, errors.New("ts is not formatted per RFC 3339")
				}
				if timeTs.Before(minTimestamp) {
					return 0, errors.New("ts before Unix epoch")
				}
				ts = toMicrosecondTime(timeTs)
			} else {
				return 0, errors.New("ts is not a string")
			}
		} else {
			return 0, errors.New("missing event ts")
		}

		hash := ""
		if hashValue, ok := event["_hash"]; ok {
			if hashString, ok := hashValue.(string); ok {
				hash = hashString
			}
		}

		formattedTs := formatTs(ts)
		idStr := string(eventKeyPrefix) + string(formattedTs[:]) + "-" + req.Tag + "-" + hash
		wb.Set(idStr, string(marshalled))
	}

	version, err := c.Version(req.Tag)
	if err != nil {
		if err == errNotFound {
			version = 0
		} else {
			return 0, errors.New("error getting tag version")
		}
	}

	version++
	wb.Set(string(versionKeyPrefix)+"-"+req.Tag, strconv.Itoa(version))

	_, err = c.lm2Col.Update(wb)
	if err != nil {
		return 0, err
	}

	return version, nil
}
