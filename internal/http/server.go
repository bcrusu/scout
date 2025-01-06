package http

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/gorilla/mux"
)

const (
	DefaultPort = 8080
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.New("http_server")
)

// ServerConfig is the HTTP server configuration.
type ServerConfig struct {
	Address           string        `yaml:"address" validate:"maxLen:128"`
	ReadTimeout       time.Duration `yaml:"readTimeout" default:"10s" validate:"positive"`
	ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout" default:"3s" validate:"positive"`
	WriteTimeout      time.Duration `yaml:"writeTimeout" default:"10s" validate:"positive"`
	EnablePprof       bool          `yaml:"enablePprof" default:"true"`
}

// Server represents the HTTP server.
type Server struct {
	config ServerConfig
	server *http.Server
}

// NewServer returns a new Server instance.
func NewServer(config ServerConfig) *Server {
	r := mux.NewRouter()

	if config.EnablePprof {
		r.HandleFunc("/pprof/", pprof.Index)
		r.HandleFunc("/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/pprof/profile", pprof.Profile)
		r.HandleFunc("/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/pprof/trace", pprof.Trace)
	}

	server := &http.Server{
		Addr:              config.Address,
		Handler:           r,
		ReadTimeout:       config.ReadTimeout,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		WriteTimeout:      config.WriteTimeout,
		MaxHeaderBytes:    1 << 20,
	}

	return &Server{
		config: config,
		server: server,
	}
}

// Start the server.
func (s *Server) Start(ctx context.Context) error {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.WithContext(ctx).WithError(err).Error("Failed to serve.")
		}
	}()

	return nil
}

// Stop the server, with no survivors.
func (s *Server) Stop() {
	if err := s.server.Close(); err != nil {
		log.WithError(err).Error("Failed to close.")
	}
}
