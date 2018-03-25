package main

import (
	"encoding/json"
	graphql "github.com/graph-gophers/graphql-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"net/http"
	"os"
	"src.katchecode.com/gcs-blob-svc/schema"
)

var env struct {
	debug   bool
	develop bool
	port    string
	bucket  string
	prefix  string

	privateKey     []byte
	gcsCredentials struct {
		PrivateKey  string `json:"private_key"`
		ClientEmail string `json:"client_email"`
	}
}

var compiledSchema *graphql.Schema

func init() {

	env.debug = (os.Getenv("DEBUG") == "true")
	env.develop = (os.Getenv("DEVELOP") == "true")
	env.port = os.Getenv("PORT")
	env.bucket = os.Getenv("GCS_BUCKET")
	env.prefix = os.Getenv("GCS_PREFIX")
	googleCredentialsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	reader, err := ioutil.ReadFile(googleCredentialsFile)
	if err != nil {

		log.Panic().
			Err(err).
			Msg("Failed to load google application credentials file " + googleCredentialsFile)
	}

	json.Unmarshal(reader, &env.gcsCredentials)

	zerolog.MessageFieldName = "msg"

	if env.debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if env.develop {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		zerolog.TimeFieldFormat = ""
	}

	log.Logger = log.With().Str("svc", "gcs-blob-svc").Logger()

	// compiledSchema = graphql.MustParseSchema(schema.String(), schema.NewResolvers(env.bucket, env.privateKey))
	compiledSchema = graphql.MustParseSchema(schema.String(), schema.NewResolver(env.bucket, env.prefix, env.gcsCredentials.ClientEmail, env.gcsCredentials.PrivateKey))
}

func startServer(port string) {

	handler := http.NewServeMux()

	handler.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler.HandleFunc("/graphql", serveHTTP)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}
	log.Info().Str("port", port).Msg("Starting server")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal().Err(err).Msg("Startup failed")
	}

}

func main() {
	log.Info().Interface("env", env).Msg("Starting up")
	startServer(env.port)
}

func serveHTTP(w http.ResponseWriter, r *http.Request) {

	var params struct {
		Query         string
		OperationName string
		Variables     map[string]interface{}
	}

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {

		log.Debug().
			Err(err).
			Msg("Error parsing request body")

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg := log.Debug().
		Str("operation-name", params.OperationName).
		Str("query", params.Query)

	if env.develop {
		msg.Interface("variables", params.Variables)
	}

	msg.Msg("Parsed request")

	response := compiledSchema.Exec(r.Context(), params.Query, params.OperationName, params.Variables)

	log.Debug().
		Str("operation-name", params.OperationName).
		Interface("response", response).
		Msg("Generated response")

	responseJSON, err := json.Marshal(response)

	if err != nil {

		msg := log.Error().
			Err(err).
			Str("operation name", params.OperationName)

		if env.develop {
			msg.Interface("variables", params.Variables)
		}

		msg.Msg("Error serializing response")

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}
