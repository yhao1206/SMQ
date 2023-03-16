package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/yhao1206/SMQ/message"
	"github.com/yhao1206/SMQ/protocol"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type ReqParams struct {
	params url.Values
	body   []byte
}

func NewReqParams(req *http.Request) (*ReqParams, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	return &ReqParams{reqParams, data}, nil
}

func (r *ReqParams) Query(key string) (string, error) {
	keyData := r.params[key]
	if len(keyData) == 0 {
		return "", errors.New("key not in query params")
	}
	return keyData[0], nil
}

func HttpServer(ctx context.Context, address string, port string, endChan chan struct{}) {
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/stats", statsHandler)

	fqAddress := address + ":" + port
	httpServer := http.Server{
		Addr: fqAddress,
	}

	go func() {
		log.Printf("listening for http requests on %s", fqAddress)
		err := http.ListenAndServe(fqAddress, nil)
		if err != nil {
			log.Fatal("http.ListenAndServe:", err)
		}
	}()

	<-ctx.Done()
	log.Printf("HTTP server on %s is shutdowning...", fqAddress)
	timeoutCtx, fn := context.WithTimeout(context.Background(), 10*time.Second)
	defer fn()
	if err := httpServer.Shutdown(timeoutCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	close(endChan)
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := NewReqParams(req)
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	conn := &FakeConn{}
	client := NewClient(conn, "HTTP")
	proto := &protocol.Protocol{}
	resp, err := proto.Execute(client, "PUB", topicName, string(reqParams.body))
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(resp)))
	w.Write(resp)
}

func statsHandler(w http.ResponseWriter, req *http.Request) {
	for topicName, _ := range message.TopicMap {
		io.WriteString(w, fmt.Sprintf("%s\n", topicName))
	}
}
