package rexster_client

import (
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
	msg := "javax.script.ScriptException: groovy.lang.MissingPropertyException: No such property: thiswillfail for class: Script3"
	if err.Error() != msg {
		t.Errorf("expected Eval to fail with message '%v', got '%v'", msg, err.Error())
	}
}
