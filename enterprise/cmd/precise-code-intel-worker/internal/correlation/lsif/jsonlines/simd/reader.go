package jsonlines

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"

	simdjson "github.com/minio/simdjson-go"
	"github.com/sourcegraph/sourcegraph/enterprise/cmd/precise-code-intel-worker/internal/correlation/datastructures"
	"github.com/sourcegraph/sourcegraph/enterprise/cmd/precise-code-intel-worker/internal/correlation/lsif"
)

// ChannelBufferSize is the number sources lines that can be read ahead of the correlator.
const ChannelBufferSize = 512

// NumUnmarshalGoRoutines is the number of goroutines launched to unmarshal individual lines.
var NumUnmarshalGoRoutines = runtime.NumCPU() * 2

// TODO
func Read(ctx context.Context, r io.Reader) <-chan lsif.Pair {
	pairCh := make(chan lsif.Pair, ChannelBufferSize)

	go func() {
		defer close(pairCh)

		reuse := make(chan *simdjson.ParsedJson, ChannelBufferSize)
		defer close(reuse)

		stream := make(chan simdjson.Stream, ChannelBufferSize)
		simdjson.ParseNDStream(r, stream, reuse)

		handle := func(elem simdjson.Stream, ch chan<- lsif.Pair) error {
			defer close(ch)

			it2 := iterPool.Get().(*simdjson.Iter)
			defer iterPool.Put(it2)

			if elem.Error != nil {
				if elem.Error != io.EOF {
					return elem.Error
				}
				return nil
			}

			it := elem.Value.Iter()

			for it.Advance() != simdjson.TypeNone {
				_, it2, err := (&it).Root(it2) // TODO - check nils
				if err != nil {
					return err
				}

				x, err := unmarshalElement(it2)
				if err != nil {
					return err
				}

				ch <- lsif.Pair{Element: x}
			}

			select {
			case reuse <- elem.Value:
			default:
			}

			return nil
		}

		queue := make(chan chan lsif.Pair, NumUnmarshalGoRoutines)

		go func() {
			defer close(queue)

			for elem := range stream {
				ch := make(chan lsif.Pair, NumUnmarshalGoRoutines)
				queue <- ch

				go func(elem simdjson.Stream) {
					if err := handle(elem, ch); err != nil {
						panic(err.Error())
					}
				}(elem)
			}
		}()

		for x := range queue {
			for y := range x {
				pairCh <- y
			}
		}
	}()

	return pairCh
}

var iterPool = &sync.Pool{New: func() interface{} { return &simdjson.Iter{} }}
var arrayPool = &sync.Pool{New: func() interface{} { return &simdjson.Array{} }}
var objectPool = &sync.Pool{New: func() interface{} { return &simdjson.Object{} }}
var elementPool = &sync.Pool{New: func() interface{} { return &simdjson.Element{} }}

func unmarshalElement(it *simdjson.Iter) (element lsif.Element, err error) {
	obj := objectPool.Get().(*simdjson.Object)
	defer objectPool.Put(obj)
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)

	obj, err = it.Object(obj)
	if err != nil {
		return lsif.Element{}, err
	}

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return lsif.Element{}, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "id" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Element{}, err
			}
			element.ID = v
		}

		if name == "type" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Element{}, err
			}
			element.Type = v
		}

		if name == "label" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Element{}, err
			}
			element.Label = v
		}
	}

	obj2, err := it.Object(obj)
	if err != nil {
		return lsif.Element{}, err
	}

	if element.Type == "edge" {
		element.Payload, err = unmarshalEdge(obj2)
	} else if element.Type == "vertex" {
		if unmarshaler, ok := vertexUnmarshalers[element.Label]; ok {
			element.Payload, err = unmarshaler(obj2)
		}
	}

	return element, err
}

func unmarshalEdge(obj *simdjson.Object) (interface{}, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)
	vx := arrayPool.Get().(*simdjson.Array)
	defer arrayPool.Put(vx)

	edge := lsif.Edge{}

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return lsif.Edge{}, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "outV" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Edge{}, err
			}
			edge.OutV = v
		}

		if name == "inV" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Edge{}, err
			}
			edge.InV = v
		}

		if name == "inVs" {
			vx, err := tmp.Array(vx)
			if err != nil {
				return lsif.Edge{}, err
			}
			y := vx.Iter()
			it := &y

			var inVs []string
			for it.Advance() != simdjson.TypeNone {
				a, err := it.String()
				if err != nil {
					return lsif.Edge{}, err
				}

				inVs = append(inVs, a)
			}

			edge.InVs = inVs
		}

		if name == "document" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Edge{}, err
			}
			edge.Document = v
		}
	}

	return edge, nil
}

var vertexUnmarshalers = map[string]func(obj *simdjson.Object) (interface{}, error){
	"metaData":           unmarshalMetaData,
	"document":           unmarshalDocument,
	"range":              unmarshalRange,
	"hoverResult":        unmarshalHover,
	"moniker":            unmarshalMoniker,
	"packageInformation": unmarshalPackageInformation,
	"diagnosticResult":   unmarshalDiagnosticResult,
}

func unmarshalMetaData(obj *simdjson.Object) (interface{}, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)

	metaData := lsif.MetaData{}

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return lsif.Element{}, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "version" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Element{}, err
			}
			metaData.Version = v
		}

		if name == "projectRoot" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Element{}, err
			}
			metaData.ProjectRoot = v
		}
	}

	return metaData, nil
}

func unmarshalDocument(obj *simdjson.Object) (interface{}, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)

	document := lsif.Document{
		Contains:    datastructures.IDSet{},
		Diagnostics: datastructures.IDSet{},
	}

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return lsif.Element{}, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "uri" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Element{}, err
			}
			document.URI = v
		}
	}

	return document, nil
}

func unmarshalRange(obj *simdjson.Object) (interface{}, error) {
	startLine, startCharacter, endLine, endCharacter, err := unmarshalStartEndPositions(obj)
	if err != nil {
		return nil, err
	}

	return lsif.Range{
		StartLine:      startLine,
		StartCharacter: startCharacter,
		EndLine:        endLine,
		EndCharacter:   endCharacter,
		MonikerIDs:     datastructures.IDSet{},
	}, nil
}

func unmarshalStartEndPositions(obj *simdjson.Object) (startLine, startCharacter, endLine, endCharacter int, _ error) {
	o2 := objectPool.Get().(*simdjson.Object)
	defer objectPool.Put(o2)
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "start" {
			o2, err := tmp.Object(o2)
			if err != nil {
				return 0, 0, 0, 0, err
			}

			startLine, startCharacter, err = unmarshalPosition(o2)
			if err != nil {
				return 0, 0, 0, 0, err
			}
		}
		if name == "end" {
			o2, err := tmp.Object(o2)
			if err != nil {
				return 0, 0, 0, 0, err
			}

			endLine, endCharacter, err = unmarshalPosition(o2)
			if err != nil {
				return 0, 0, 0, 0, err
			}
		}
	}

	return startLine, startCharacter, endLine, endCharacter, nil
}

func unmarshalPosition(obj *simdjson.Object) (int, int, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)

	var line, character int64

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return 0, 0, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "line" {
			line, err = tmp.Int()
			if err != nil {
				return 0, 0, err
			}
		}
		if name == "character" {
			character, err = tmp.Int()
			if err != nil {
				return 0, 0, err
			}
		}
	}

	return int(line), int(character), nil
}

func unmarshalHover(obj *simdjson.Object) (interface{}, error) {
	y := arrayPool.Get().(*simdjson.Array)
	defer arrayPool.Put(y)
	o2 := objectPool.Get().(*simdjson.Object)
	defer objectPool.Put(o2)
	elem := elementPool.Get().(*simdjson.Element)
	defer elementPool.Put(elem)

	elem = obj.FindKey("result", elem) // TODO - null checks
	o2, err := (&elem.Iter).Object(o2)
	if err != nil {
		return nil, err
	}
	cx := o2.FindKey("contents", elem)

	if (cx.Iter).Type() == simdjson.TypeArray {
		y, err := (&cx.Iter).Array(y)
		if err != nil {
			return nil, err
		}
		z := y.Iter()
		it := &z

		var parts []string
		for it.Advance() != simdjson.TypeNone {
			part, err := unmarshalHoverPart(it)
			if err != nil {
				return "", err
			}

			parts = append(parts, part)
		}

		return strings.Join(parts, "\n\n---\n\n"), nil
	}

	return unmarshalHoverPart(&cx.Iter)
}

func unmarshalHoverPart(it *simdjson.Iter) (string, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)
	obj := objectPool.Get().(*simdjson.Object)
	defer objectPool.Put(obj)

	if it.Type() == simdjson.TypeString {
		return it.String()
	}

	obj, err := it.Object(obj)
	if err != nil {
		return "", err
	}

	var language, value string

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return "", err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "language" {
			language, err = tmp.String()
			if err != nil {
				return "", err
			}
		}
		if name == "value" {
			value, err = tmp.String()
			if err != nil {
				return "", err
			}
		}
	}

	if language != "" {
		return fmt.Sprintf("```%s\n%s\n```", language, value), nil
	}

	return strings.TrimSpace(value), nil
}

func unmarshalMoniker(obj *simdjson.Object) (interface{}, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)

	moniker := lsif.Moniker{}

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return nil, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "kind" {
			v, err := tmp.String()
			if err != nil {
				return nil, err
			}
			moniker.Kind = v
		}

		if name == "scheme" {
			v, err := tmp.String()
			if err != nil {
				return nil, err
			}
			moniker.Scheme = v
		}

		if name == "identifier" {
			v, err := tmp.String()
			if err != nil {
				return nil, err
			}
			moniker.Identifier = v
		}
	}

	return moniker, nil
}

func unmarshalPackageInformation(obj *simdjson.Object) (interface{}, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)

	packageInformation := lsif.PackageInformation{}

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return nil, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "name" {
			v, err := tmp.String()
			if err != nil {
				return nil, err
			}
			packageInformation.Name = v
		}

		if name == "version" {
			v, err := tmp.String()
			if err != nil {
				return nil, err
			}
			packageInformation.Version = v
		}
	}

	return packageInformation, nil
}

func unmarshalDiagnosticResult(obj *simdjson.Object) (interface{}, error) {
	a := arrayPool.Get().(*simdjson.Array)
	defer arrayPool.Put(a)
	o2 := objectPool.Get().(*simdjson.Object)
	defer objectPool.Put(o2)
	elem := elementPool.Get().(*simdjson.Element)
	defer elementPool.Put(elem)

	elem = obj.FindKey("result", elem)
	a, err := (&elem.Iter).Array(a)
	if err != nil {
		return nil, err
	}
	y := a.Iter()
	it := &y

	var diagnostics []lsif.Diagnostic

	for it.Advance() != simdjson.TypeNone {
		o2, err = it.Object(o2)
		if err != nil {
			return nil, err
		}

		diagnostic, err := unmarshalDiagnostic(o2)
		if err != nil {
			return nil, err
		}

		diagnostics = append(diagnostics, diagnostic)
	}

	return lsif.DiagnosticResult{Result: diagnostics}, nil
}

func unmarshalDiagnostic(obj *simdjson.Object) (lsif.Diagnostic, error) {
	tmp := iterPool.Get().(*simdjson.Iter)
	defer iterPool.Put(tmp)
	x := objectPool.Get().(*simdjson.Object)
	defer objectPool.Put(x)

	diagnostic := lsif.Diagnostic{}

	for {
		name, t, err := obj.NextElement(tmp)
		if err != nil {
			return lsif.Diagnostic{}, err
		}
		if t == simdjson.TypeNone {
			// Done
			break
		}

		if name == "severity" {
			v, err := tmp.Int()
			if err != nil {
				return lsif.Diagnostic{}, err
			}
			diagnostic.Severity = int(v)
		}

		if name == "code" {
			v, err := tmp.Int()
			if err != nil {
				return lsif.Diagnostic{}, err
			}
			diagnostic.Code = fmt.Sprintf("%d", v)
		}

		if name == "message" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Diagnostic{}, err
			}
			diagnostic.Message = v
		}

		if name == "source" {
			v, err := tmp.String()
			if err != nil {
				return lsif.Diagnostic{}, err
			}
			diagnostic.Source = v
		}

		if name == "range" {
			x, err := tmp.Object(x)
			if err != nil {
				return lsif.Diagnostic{}, err
			}

			startLine, startCharacter, endLine, endCharacter, err := unmarshalStartEndPositions(x)
			if err != nil {
				return lsif.Diagnostic{}, err
			}

			diagnostic.StartLine = startLine
			diagnostic.StartCharacter = startCharacter
			diagnostic.EndLine = endLine
			diagnostic.EndCharacter = endCharacter
		}
	}

	return diagnostic, nil
}
