package mapping

import _ "embed"

// SchemaJSON is the JSON Schema of a mapping document, for editors and agents
// that want to check a draft before sending it. The compiler is the authority:
// the schema cannot express path syntax limits or unique ids.
//
//go:embed schema/mapping.schema.json
var SchemaJSON []byte

// ExampleAcmePGW is a complete, tested example mapping.
//
//go:embed examples/acme-pgw.yaml
var ExampleAcmePGW []byte
