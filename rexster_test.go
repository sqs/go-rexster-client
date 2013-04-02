package rexster_client

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// Run this test against a Rexster server containing its built-in
// sample graphs. In the rexster repo, run `./rexster.sh -s` to start
// the server with the sample graphs.
var testG = Graph{
	Name: "tinkergraph",
	Server: Rexster{
		Host:     "127.0.0.1",
		RestPort: 8182,
		Debug:    true,
	},
}

func TestGetVertex(t *testing.T) {
	r, err := testG.GetVertex("3")
	if err != nil {
		t.Fatal("failed to get vertex:", err)
	}
	if v := r.Vertex(); v.Id() != "3" {
		t.Errorf("expected _id=3, got %v", v.Id())
	}

	// try to get a non-existent vertex
	r, err = testG.GetVertex("doesnotexist")
	if err == nil {
		t.Fatal("expected GetVertex to fail, got resp:", r)
	}
	msg := "Vertex with [doesnotexist] cannot be found."
	if err.Error() != msg {
		t.Errorf("expected GetVertex to fail with message '%v', got '%v'", msg, err.Error())
	}
}

func TestGetVertexURL(t *testing.T) {
	u := testG.getVertexURL("has/a/slash")
	wantUrl := "http://127.0.0.1:8182/graphs/tinkergraph/vertices/has%252Fa%252Fslash"
	if u != wantUrl {
		t.Errorf("want %s, got %s", wantUrl, u)
	}
}

func TestQueryVertices(t *testing.T) {
	r, err := testG.QueryVertices("lang", "java")
	if err != nil {
		t.Fatal("failed to query vertices:", err)
	}
	if vs := r.Vertices(); vs != nil {
		want := []*Vertex{
			&Vertex{Map: map[string]interface{}{"_type": "vertex", "name": "lop", "_id": "3", "lang": "java"}},
			&Vertex{Map: map[string]interface{}{"lang": "java", "name": "ripple", "_id": "5", "_type": "vertex"}},
		}
		if !verticesEqualsVertices(vs, want) {
			t.Errorf("want %#v, got %#v", verticesToString(want), verticesToString(vs))
		}
	} else {
		t.Errorf("vertices was nil")
	}
}

func TestQueryVerticesBatch(t *testing.T) {
	r, err := testG.QueryVerticesBatch("name", []string{"peter", "vadas"})
	if err != nil {
		t.Fatal("failed to query vertices batch:", err)
	}
	if vs := r.Vertices(); vs != nil {
		want := []*Vertex{
			&Vertex{Map: map[string]interface{}{"_type": "vertex", "name": "peter", "_id": "6", "age": float64(35)}},
			&Vertex{Map: map[string]interface{}{"age": float64(27), "name": "vadas", "_id": "2", "_type": "vertex"}},
		}
		if !verticesEqualsVertices(vs, want) {
			t.Errorf("want %#v, got %#v", verticesToString(want), verticesToString(vs))
		}
	} else {
		t.Errorf("vertices was nil")
	}
}

func vertexEqualsVertex(v1 *Vertex, v2 *Vertex) bool {
	return reflect.DeepEqual(*v1, *v2)
}

func verticesEqualsVertices(vs1 []*Vertex, vs2 []*Vertex) bool {
	if len(vs1) != len(vs2) {
		return false
	}
	for i, v1 := range vs1 {
		v2 := vs2[i]
		if !vertexEqualsVertex(v1, v2) {
			return false
		}
	}
	return true
}

func verticesToString(vs []*Vertex) string {
	s := ""
	for i, v := range vs {
		s += fmt.Sprintf("%v", *v)
		if i != len(vs)-1 {
			s += ", "
		}
	}
	return s
}

func edgeEqualsEdge(e1 *Edge, e2 *Edge) bool {
	return reflect.DeepEqual(*e1, *e2)
}

func edgesEqualEdges(es1 []*Edge, es2 []*Edge) bool {
	if len(es1) != len(es2) {
		return false
	}
	for i, e1 := range es1 {
		e2 := es2[i]
		if !edgeEqualsEdge(e1, e2) {
			return false
		}
	}
	return true
}

func edgesToString(es []*Edge) string {
	s := ""
	for i, e := range es {
		s += fmt.Sprintf("%v", *e)
		if i != len(es)-1 {
			s += ", "
		}
	}
	return s
}

func TestGetVertexInE(t *testing.T) {
	r, err := testG.GetVertexInE("2")
	if err != nil {
		t.Fatal("failed to get vertex inE:", err)
	}
	if es := r.Edges(); es != nil {
		want := []*Edge{
			&Edge{Map: map[string]interface{}{"_outV": "1", "_id": "7", "_type": "edge", "_inV": "2", "_label": "knows", "weight": 0.5}},
		}
		if !edgesEqualEdges(es, want) {
			t.Errorf("want %#v, got %#v", edgesToString(want), edgesToString(es))
		}
	} else {
		t.Errorf("edges was nil")
	}
}

func TestGetEdge(t *testing.T) {
	r, err := testG.GetEdge("12")
	if err != nil {
		t.Fatal("failed to get edge:", err)
	}
	if e := r.Edge(); e.Id() != "12" {
		t.Errorf("expected _id=12, got %v", e.Id())
	}

	// try to get a non-existent edge
	r, err = testG.GetEdge("doesnotexist")
	if err == nil {
		t.Fatal("expected GetEdge to fail, got resp:", r)
	}
	msg := "Edge with id [doesnotexist] cannot be found."
	if err.Error() != msg {
		t.Errorf("expected GetEdge to fail with message '%v', got '%v'", msg, err.Error())
	}
}

func TestQueryEdges(t *testing.T) {
	key := uniqueId("TestQueryEdges_key")
	r, err := testG.CreateKeyIndex(EdgeKeyIndex, key)
	if err != nil {
		t.Fatalf("failed to create edge key index for key %s: %v", key, err)
	}

	// make an edge (with outV/inV)
	outV := NewVertex(uniqueId("TestCreateOrUpdateEdge_outV"), nil)
	inV := NewVertex(uniqueId("TestCreateOrUpdateEdge_inV"), nil)
	r, _ = testG.CreateOrUpdateVertex(outV)
	r, _ = testG.CreateOrUpdateVertex(inV)
	e := NewEdge(uniqueId("TestQueryEdges_edge"), outV.Id(), "created", inV.Id(), map[string]interface{}{key: "foo", "_type": "edge"})
	r, _ = testG.CreateOrUpdateEdge(e)

	r, err = testG.QueryEdges(key, "foo")
	if err != nil {
		t.Fatal("failed to query edges:", err)
	}
	if es := r.Edges(); es != nil {
		want := []*Edge{
			e,
			// TODO(sqs): expected data...
		}
		if !edgesEqualEdges(es, want) {
			t.Errorf("want %#v, got %#v", edgesToString(want), edgesToString(es))
		}
	} else {
		t.Errorf("edges was nil")
	}
}

func TestCreateOrUpdateVertex(t *testing.T) {
	v := NewVertex(uniqueId("TestCreateOrUpdateVertex"), map[string]interface{}{"color": "blue"})

	// make sure it doesn't already exist
	r, err := testG.GetVertex(v.Id())
	if err == nil || r != nil {
		t.Fatal("expected vertex to not already exist, but it does", r.Vertex())
	}

	// create it
	r, err = testG.CreateOrUpdateVertex(v)
	if err != nil {
		t.Fatal("failed to create vertex:", err)
	}
	if v.Id() != r.Vertex().Id() {
		// this could also be caused by the graph DB implementation not
		// supporting custom IDs (e.g., neo4j).
		t.Errorf("created vertex %v has a different id from what we created, %v", r.Vertex(), v)
	}
	if v.Get("color") != r.Vertex().Get("color") {
		t.Errorf("created vertex %v has a different color from what we created, %v", r.Vertex(), v)
	}

	// update it
	v.Map["color"] = "red"
	v.Map["radius"] = 3
	r, err = testG.CreateOrUpdateVertex(v)
	if err != nil {
		t.Fatal("failed to update vertex:", err)
	}
	if v.Get("color") != r.Vertex().Get("color") {
		t.Errorf("created vertex %v has a different color from what we updated, %v", r.Vertex(), v)
	}
	if v.Map["radius"].(int) != int(r.Vertex().Map["radius"].(float64)) {
		t.Errorf("created vertex %v has a different radius from what we updated, %v", r.Vertex(), v)
	}
}

func TestCreateOrUpdateEdge(t *testing.T) {
	outV := NewVertex(uniqueId("TestCreateOrUpdateEdge_outV"), nil)
	inV := NewVertex(uniqueId("TestCreateOrUpdateEdge_inV"), nil)
	r, _ := testG.CreateOrUpdateVertex(outV)
	r, _ = testG.CreateOrUpdateVertex(inV)

	e := NewEdge(uniqueId("TestCreateOrUpdateEdge"), outV.Id(), "TestLabel", inV.Id(), map[string]interface{}{"color": "blue"})

	// make sure it doesn't already exist
	r, err := testG.GetEdge(e.Id())
	if err == nil || r != nil {
		t.Fatal("expected edge to not already exist, but it does", r.Edge())
	}

	// create it
	r, err = testG.CreateOrUpdateEdge(e)
	if err != nil {
		t.Fatal("failed to create edge:", err, e.Map)
	}
	if e.Id() != r.Edge().Id() {
		// this could also be caused by the graph DB implementation not
		// supporting custom IDs (e.g., neo4j).
		t.Errorf("created edge %v has a different id from what we created, %v", r.Edge(), e)
	}
	if e.Get("color") != r.Edge().Get("color") {
		t.Errorf("created edge %v has a different color from what we created, %v", r.Edge(), e)
	}

	// update it
	e.Map["color"] = "red"
	e.Map["radius"] = 3
	r, err = testG.CreateOrUpdateEdge(e)
	if err != nil {
		t.Fatal("failed to update edge:", err)
	}
	if e.Get("color") != r.Edge().Get("color") {
		t.Errorf("created edge %v has a different color from what we updated, %v", r.Edge(), e)
	}
	if e.Map["radius"].(int) != int(r.Edge().Map["radius"].(float64)) {
		t.Errorf("created edge %v has a different radius from what we updated, %v", r.Edge(), e)
	}
}

func TestEval(t *testing.T) {
	r, err := testG.Eval("g.V[3]")
	if err != nil {
		t.Fatal("failed to eval:", err)
	}
	if !r.Success {
		t.Error("resp failure:", r)
	}

	// try to eval a script that will trigger an error
	r, err = testG.Eval("thiswillfail")
	if err == nil {
		t.Fatal("expected Eval to fail, got resp:", r)
	}
}

func TestCreateKeyIndex(t *testing.T) {
	r, err := testG.CreateKeyIndex(VertexKeyIndex, "foo")
	if err != nil || r == nil {
		t.Error("failed to create vertex key index:", err)
	}

	r, err = testG.CreateKeyIndex(EdgeKeyIndex, "foo")
	if err != nil || r == nil {
		t.Error("failed to create edge key index:", err)
	}

	// TODO(sqs): actually test post-conditions instead of just
	// ensuring no error
}

func uniqueId(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}
