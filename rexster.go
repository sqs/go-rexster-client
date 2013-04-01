package rexster_client

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
)

// Rexster API

type Rexster struct {
	Host     string // Rexster server host
	RestPort uint16 // Rexster server REST API port (usually 8182)
	Debug    bool   // Enable debug logging
}

type Graph struct {
	Name   string  // Name of graph served by Rexster
	Server Rexster // The Rexster server that serves this graph
}

type Response struct {
	Results   interface{} `json:"results"`
	Success   bool        `json:"success"`
	Version   string      `json:"version"`
	QueryTime float64     `json:"queryTime"`
}

type errorResponse struct {
	Message string `json:"message"`
	Error   string `json:"error"`
}

func (g Graph) GetVertex(id string) (res *Response, err error) {
	g.log("GET VERTEX", id)
	url := g.getVertexURL(id)
	return g.Server.send(url)
}

func (g Graph) Eval(script string) (res *Response, err error) {
	g.log("EVAL", script)
	url := g.evalURL(script)
	return g.Server.send(url)
}

func (g Graph) log(v ...interface{}) {
	if g.Server.Debug {
		vs := []interface{}{"GRAPH", g.Name}
		vs = append(vs, v...)
		log.Println(vs...)
	}
}

func (r Rexster) send(url string) (resp *Response, err error) {
	hr, err := http.Get(url)
	if err != nil {
		if r.Debug {
			log.Println("HTTP GET failed", url, err)
		}
		return nil, err
	}
	resp, errResp := readResponseOrError(hr)
	if errResp != nil {
		err = errors.New(strings.TrimSpace(strings.Join([]string{errResp.Message, errResp.Error}, " ")))
		if r.Debug {
			log.Println("HTTP GET failed", url, err)
		}
	}
	return
}

func readResponseOrError(hr *http.Response) (resp *Response, errResp *errorResponse) {
	dec := json.NewDecoder(hr.Body)
	defer hr.Body.Close()
	if hr.StatusCode == 200 {
		resp = new(Response)
		dec.Decode(resp)
	} else {
		errResp = new(errorResponse)
		dec.Decode(errResp)
	}
	return
}

// URLs

func (r Rexster) baseURL() *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", r.Host, r.RestPort),
	}
}

func (g Graph) baseURL() *url.URL {
	u := g.Server.baseURL()
	u.Path = "/graphs/" + g.Name
	return u
}

func (g Graph) getVertexURL(id string) string {
	u := g.baseURL()
	u.Path += "/vertices/"
	return u.String() + strings.Replace(id, "/", "%2F", -1) // escape slashes
}

func (g Graph) evalURL(script string) string {
	u := g.baseURL()
	u.Path += "/tp/gremlin"
	q := url.Values{"script": []string{script}}
	u.RawQuery = q.Encode()
	return u.String()
}

// Data

type Vertex struct {
	Map map[string]interface{}
}

// Vertex() gets the single vertex in the response. If the response
// does not contain a single vertex (i.e., if it contains multiple
// vertices, or a different data type), Vertex() returns nil.
func (r *Response) Vertex() (v *Vertex) {
	if v, ok := r.Results.(map[string]interface{}); ok && v["_type"] == "vertex" {
		return &Vertex{v}
	} else {
		return nil
	}
}

func (v Vertex) Id() string {
	return v.Map["_id"].(string)
}
