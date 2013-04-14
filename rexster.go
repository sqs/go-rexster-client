// go-rexster-client is a Rexster graph database client for Go. See
// https://github.com/tinkerpop/rexster/wiki for more information
// about Rexster.
//
// It implements a subset of the
// Rexster REST API: https://github.com/tinkerpop/rexster/wiki/Basic-REST-API.
//
// To use the *Batch functions, you must have the batch kibble
// installed. See
// https://github.com/tinkerpop/rexster/tree/master/rexster-kibbles/batch-kibble
// for more information. In the Rexster source dir, this means copying
// batch-kibble-2.4.0-SNAPSHOT.jar to
// ./rexster-server/target/rexster-server-2.4.0-SNAPSHOT-standalone/lib/.
package rexster_client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	g.log("GetVertex", id)
	url := g.getVertexURL(id)
	return g.Server.get(url)
}

func (g Graph) QueryVertices(key, value string) (res *Response, err error) {
	g.log("QueryVertices", key, value)
	url := g.queryVerticesURL(key, value)
	return g.Server.get(url)
}

// QueryVerticesBatch retrieves all vertices in a key index with any
// of the specified values. Requires the batch kibble.
func (g Graph) QueryVerticesBatch(key string, values []string) (res *Response, err error) {
	g.log("QueryVerticesBatch", key, len(values))
	url := g.queryVerticesBatchURL(key, values)
	return g.Server.get(url)
}

func (g Graph) GetVertexBothE(id string) (res *Response, err error) {
	g.log("GetVertexBothE", id)
	url := g.getVertexSubURL(id, "bothE")
	return g.Server.get(url)
}

func (g Graph) GetVertexInE(id string) (res *Response, err error) {
	g.log("GetVertexInE", id)
	url := g.getVertexSubURL(id, "inE")
	return g.Server.get(url)
}

func (g Graph) GetVertexOutE(id string) (res *Response, err error) {
	g.log("GetVertexOutE", id)
	url := g.getVertexSubURL(id, "outE")
	return g.Server.get(url)
}

func (g Graph) GetEdge(id string) (res *Response, err error) {
	g.log("GetEdge", id)
	url := g.getEdgeURL(id)
	return g.Server.get(url)
}

func (g Graph) QueryEdges(key, value string) (res *Response, err error) {
	g.log("QueryEdges", key, value)
	url := g.queryEdgesURL(key, value)
	return g.Server.get(url)
}

// TODO(sqs): allow passing params to obviate interpolation/avoid
// injection attacks
func (g Graph) Eval(script string) (res *Response, err error) {
	g.log("Eval", script)
	url := g.evalURL(script)
	return g.Server.get(url)
}

func (g Graph) CreateOrUpdateVertex(v *Vertex) (res *Response, err error) {
	g.log("CreateOrUpdateVertex", v.Id())
	url := g.getVertexURL(v.Id())
	return g.Server.send("POST", url, v.Map)
}

func (g Graph) CreateOrUpdateEdge(e *Edge) (res *Response, err error) {
	g.log("CreateOrUpdateEdge", e)
	url := g.getEdgeURL(e.Id())
	return g.Server.send("POST", url, e.Map)
}

type VertexOrEdge interface {
	Id() string
	GetMap() map[string]interface{}
	Type() string
}

type TxActionType string

const (
	Create TxActionType = "create"
	Update              = "update"
	Delete              = "delete"
)

type TxAction struct {
	Item VertexOrEdge
	Type TxActionType
}

// Execute a batch transaction. See
// https://github.com/tinkerpop/rexster/tree/master/rexster-kibbles/batch-kibble.
// TODO(sqs): support update/delete
func (g Graph) Batch(actions []TxAction) (res *Response, err error) {
	g.log("Batch", len(actions))
	actionData := make([]map[string]interface{}, len(actions))
	for i, a := range actions {
		actionData[i] = make(map[string]interface{}, len(a.Item.GetMap()))
		for k, v := range a.Item.GetMap() {
			actionData[i][k] = v
		}
		if a.Item.Id() != "" {
			actionData[i]["_id"] = a.Item.Id()
		}
		actionData[i]["_type"] = a.Item.Type()
		actionData[i]["_action"] = string(a.Type)
	}
	return g.Server.send("POST", g.batchTxUrl(), map[string]interface{}{"tx": actionData})
}

type KeyIndexType int

const (
	VertexKeyIndex KeyIndexType = iota
	EdgeKeyIndex
)

func (g Graph) CreateKeyIndex(type_ KeyIndexType, key string) (res *Response, err error) {
	g.log("CreateKeyIndex", key)
	url := g.getKeyIndexURL(type_, key)
	return g.Server.send("POST", url, nil)
}

func (g Graph) log(v ...interface{}) {
	if g.Server.Debug {
		vs := []interface{}{"GRAPH", g.Name}
		vs = append(vs, v...)
		log.Println(vs...)
	}
}

func (r Rexster) get(url string) (resp *Response, err error) {
	return r.send("GET", url, nil)
}

func (r Rexster) send(method string, url string, data map[string]interface{}) (resp *Response, err error) {
	var body io.Reader
	if data != nil {
		buf, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	hr, err := http.DefaultClient.Do(req)
	if err != nil {
		if r.Debug {
			log.Printf("HTTP %s failed to %s: %v", method, url, err)
		}
		return nil, err
	}
	resp, errResp := readResponseOrError(hr)
	if errResp != nil {
		err = errors.New(strings.TrimSpace(strings.Join([]string{errResp.Message, errResp.Error}, " ")))
		if r.Debug {
			log.Printf("HTTP %s failed to %s: %v", method, url, err)
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
	return u.String() + escapeSlashes(id)
}

func (g Graph) queryVerticesURL(key, value string) string {
	u := g.baseURL()
	u.Path += "/vertices"
	q := url.Values{"key": {key}, "value": {value}}
	u.RawQuery = q.Encode()
	return u.String()
}

func (g Graph) queryVerticesBatchURL(key string, values []string) string {
	// TODO(sqs): handle commas in values
	valuesArray := "[" + strings.Join(values, ",") + "]"
	u := g.baseURL()
	u.Path += "/tp/batch/vertices"
	q := url.Values{"type": {"keyindex"}, "key": {key}, "values": {valuesArray}}
	u.RawQuery = q.Encode()
	return u.String()
}

func (g Graph) getVertexSubURL(id, subresource string) string {
	u := g.getVertexURL(id)
	return u + "/" + subresource
}

func (g Graph) getEdgeURL(id string) string {
	u := g.baseURL()
	u.Path += "/edges/"
	return u.String() + escapeSlashes(id)
}

func (g Graph) queryEdgesURL(key, value string) string {
	u := g.baseURL()
	u.Path += "/edges"
	q := url.Values{"key": {key}, "value": {value}}
	u.RawQuery = q.Encode()
	return u.String()
}

func (g Graph) evalURL(script string) string {
	u := g.baseURL()
	u.Path += "/tp/gremlin"
	q := url.Values{"script": []string{script}}
	u.RawQuery = q.Encode()
	return u.String()
}

func (g Graph) batchTxUrl() string {
	u := g.baseURL()
	u.Path += "/tp/batch/tx"
	return u.String()
}

func (g Graph) getKeyIndexURL(type_ KeyIndexType, key string) string {
	var typeName string
	if type_ == VertexKeyIndex {
		typeName = "vertex"
	} else if type_ == EdgeKeyIndex {
		typeName = "edge"
	}
	u := g.baseURL()
	u.Path += "/keyindices/" + typeName + "/" + key
	return u.String()
}

// Go's url.URL.String() improperly(?) redundantly escapes slashes in
// the path that are already percent-escaped.
func escapeSlashes(s string) string {
	return strings.Replace(strings.Replace(s, "/", "%2F", -1), "%", "%25", -1)
}

// Data

type Vertex struct {
	Map map[string]interface{}
}

func (v Vertex) GetMap() map[string]interface{} { return v.Map }
func (v Vertex) Type() string                   { return "vertex" }

func NewVertex(id string, properties map[string]interface{}) (v *Vertex) {
	if properties == nil {
		properties = make(map[string]interface{}, 0)
	}
	v = &Vertex{properties}
	v.Map["_id"] = id
	return
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
	return fmt.Sprintf("%v", v.Map["_id"])
}

func (v Vertex) Get(key string) string {
	if x, ok := v.Map[key]; ok {
		if s, ok := x.(string); ok {
			return s
		}
	}
	return ""
}

// Vertices() gets the array of vertices in the response. If the
// response does not contain an array of vertices (i.e., if it
// contains a single vertex not in an array, or a different data
// type), Vertices() returns nil.
func (r *Response) Vertices() (vs []*Vertex) {
	if vv, ok := r.Results.([]interface{}); ok {
		vs = make([]*Vertex, len(vv))
		for i, v := range vv {
			if v, ok := v.(map[string]interface{}); ok && v["_type"] == "vertex" {
				vs[i] = &Vertex{v}
			} else {
				return nil
			}
		}
	}
	return
}

type Edge struct {
	Map map[string]interface{}
}

func (e Edge) GetMap() map[string]interface{} { return e.Map }

func (e Edge) Type() string { return "edge" }

func NewEdge(id, outV, label, inV string, properties map[string]interface{}) (e *Edge) {
	if properties == nil {
		properties = make(map[string]interface{}, 0)
	}
	e = &Edge{properties}
	e.Map["_id"] = id
	e.Map["_outV"] = outV
	e.Map["_label"] = label
	e.Map["_inV"] = inV
	return
}

// Edges() gets the array of edges in the response. If the
// response does not contain an array of edges (i.e., if it
// contains a single edge not in an array, or a different data
// type), Edges() returns nil.
func (r *Response) Edges() (es []*Edge) {
	if ee, ok := r.Results.([]interface{}); ok {
		es = make([]*Edge, len(ee))
		for i, e := range ee {
			if e, ok := e.(map[string]interface{}); ok && e["_type"] == "edge" {
				es[i] = &Edge{e}
			} else {
				return nil
			}
		}
	}
	return
}

// Edge() gets the single edge in the response. If the response does
// not contain a single edge (i.e., if it contains multiple edges, or
// a different data type), Edge() returns nil.
func (r *Response) Edge() (e *Edge) {
	if e, ok := r.Results.(map[string]interface{}); ok && e["_type"] == "edge" {
		return &Edge{e}
	} else {
		return nil
	}
}

func (e Edge) Id() string {
	return fmt.Sprintf("%v", e.Map["_id"])
}

func (e Edge) Get(key string) string {
	if x, ok := e.Map[key]; ok {
		if s, ok := x.(string); ok {
			return s
		}
	}
	return ""
}
