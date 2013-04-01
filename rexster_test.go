package rexster_client

import (
	"fmt"
	"reflect"
	"testing"
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
	wantUrl := "http://127.0.0.1:8182/graphs/tinkergraph/vertices/has%2Fa%2Fslash"
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

func TestQueryEdges(t *testing.T) {
	t.SkipNow() // TODO(sqs): need to add an edge key index to test this
	r, err := testG.QueryEdges("_label", "created")
	if err != nil {
		t.Fatal("failed to query edges:", err)
	}
	if es := r.Edges(); es != nil {
		want := []*Edge{
		// TODO(sqs): expected data...
		}
		if !edgesEqualEdges(es, want) {
			t.Errorf("want %#v, got %#v", edgesToString(want), edgesToString(es))
		}
	} else {
		t.Errorf("edges was nil")
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
