package gosupabase

import (
	"errors"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// TODO: loading missing data will only load X number of rows
// TODO: handle errors properly
// TODO: Add channel to feedback the errors that are happening
// TODO: It currently uses both the commit timestamp and the created at column which is inconsistent

type Realtime struct {
	ApiKey string
	ProjectCode string

	Schema string
	Table string

	Connection *websocket.Conn
	Timestamp *time.Time
	Errors []error

	Finished bool
}

type RealTimeAction interface {
	Run(result gjson.Result) error
}

func NewRealtime(apiKey, projectCode, schema, table string) Realtime {
	return Realtime{
		apiKey,
		projectCode,
		schema,
		table,
		nil,
		nil,
		[]error{},
		false,
	}
}

func (w *Realtime) Start(lastTimestamp *time.Time, f RealTimeAction) {
	log.Println("Starting Stream")
	if lastTimestamp == nil {
		log.Println("First Run")
	} else {
		log.Println("Start: " + lastTimestamp.String())
	}

	w.Connection = w.connect()
	w.Timestamp = lastTimestamp

	log.Println("Staring Streaming ...")
	for {
		w.run(f)

		if w.Finished {
			return
		}

		// Reconnect
		w.Connection = w.connect()
	}
}

func (w *Realtime) connect() *websocket.Conn {
	log.Println("Connecting")
	c, _, err := websocket.DefaultDialer.Dial("wss://" + w.ProjectCode + ".supabase.co/realtime/v1/websocket?apikey=" + w.ApiKey + "&vsn=1.0.0", nil)

	if err != nil {
		w.Errors = append(w.Errors, err)
		return nil
	}

	return c
}

func (w *Realtime) run(f RealTimeAction) {

	if w.Connection == nil {
		w.Errors = append(w.Errors, errors.New("missing a connection"))
		return
	}

	w.Connection.WriteMessage(websocket.TextMessage, []byte(`{"topic": "realtime:` + w.Schema + `:` + w.Table + `", "event": "phx_join", "payload": {}, "ref": null}`))
	defer w.Connection.Close()

	for {
		_, message, err := w.Connection.ReadMessage()

		if err != nil {
			w.Errors = append(w.Errors, err)
			return
		}

		eventName := gjson.Get(string(message), "event")
		commitTimestamp := gjson.Get(string(message), "payload.commit_timestamp")

		if eventName.String() == "phx_reply" {

			if w.Timestamp == nil {
				log.Println("Start new")
			} else {
				log.Println("Load Missing Events from: " + w.Timestamp.String())

				layout := "2006-01-02T15:04:05"
				t2 := w.Timestamp.Format(layout)
				t3 := time.Now().UTC().Format(layout)

				client := &http.Client{}
				req, _ := http.NewRequest("GET", "https://" + w.ProjectCode + ".supabase.co/rest/v1/activity?and=(created_at.gte." + t2 + ",created_at.lt." + t3 + ")", nil)
				req.Header.Set("apikey", w.ApiKey)
				req.Header.Set("Accept-Profile", w.Schema)

				resp, err := client.Do(req)
				if err != nil {
					w.Errors = append(w.Errors, err)
					return
				}
				//We Read the response body on the line below.
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					w.Errors = append(w.Errors, err)
					return
				}

				result := gjson.Parse(string(body))

				for _, row := range result.Array() {
					err := f.Run(row)

					if err != nil {
						w.Errors = append(w.Errors, err)
						return
					}
				}
			}
		}

		if eventName.String() == "INSERT" && commitTimestamp.Exists() {

			layout := "2006-01-02T15:04:05Z"
			t, err := time.Parse(layout, commitTimestamp.String())
			if err != nil {
				w.Errors = append(w.Errors, err)
				return
			}

			err = f.Run(gjson.Get(string(message), "payload.record"))
			if err != nil {
				w.Errors = append(w.Errors, err)
				return
			}

			t = t.Add(time.Second)
			w.Timestamp = &t

		}

	}
}

func (w *Realtime) Stop() {
	log.Println("Stopping Stream")
	w.Connection.Close()
	w.Finished = true
}
