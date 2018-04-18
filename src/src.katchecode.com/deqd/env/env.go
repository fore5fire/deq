package env

import "os"

var (
	// Debug indicates if debug mode is set
	Debug = os.Getenv("DEBUG") == "true"

	// Develop indicates if development mode is set
	Develop = os.Getenv("DEVELOP") == "true"

	// Port is the port to listen on
	Port = os.Getenv("PORT")

	// Dir is the database directory
	Dir = os.Getenv("DATA_DIR")

	// ProtobufType is the protobuf type enforced by the server
	ProtobufType = os.Getenv("PROTOBUF_TYPE")
)
