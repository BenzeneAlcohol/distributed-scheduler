package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	schedulerv1 "github.com/muthuku37/distributed-scheduler/gen/scheduler/v1"
	"github.com/muthuku37/distributed-scheduler/internal/scheduler/coordinator"
	"github.com/muthuku37/distributed-scheduler/internal/scheduler/grpcapi"
	"github.com/muthuku37/distributed-scheduler/internal/scheduler/httpapi"
	postgresstore "github.com/muthuku37/distributed-scheduler/internal/scheduler/store/postgres"
	"google.golang.org/grpc"
)

const (
	defaultHTTPAddress = ":8080"
	defaultGRPCAddress = ":9090"
	defaultDatabaseURL = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"
	startupTimeout     = 10 * time.Second
	shutdownTimeout    = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	address := os.Getenv("SCHEDULER_ADDRESS")
	if address == "" {
		address = defaultHTTPAddress
	}

	grpcAddress := os.Getenv("SCHEDULER_GRPC_ADDRESS")
	if grpcAddress == "" {
		grpcAddress = defaultGRPCAddress
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultDatabaseURL
	}

	startupContext, cancelStartup := context.WithTimeout(context.Background(), startupTimeout)
	defer cancelStartup()

	jobStore, err := postgresstore.Open(startupContext, databaseURL)
	if err != nil {
		return fmt.Errorf("open job store: %w", err)
	}
	defer jobStore.Close()

	if err := jobStore.Migrate(startupContext); err != nil {
		return fmt.Errorf("migrate job store: %w", err)
	}

	jobCoordinator := coordinator.New(jobStore)
	httpServer := &http.Server{
		Addr:              address,
		Handler:           httpapi.NewHandler(jobCoordinator),
		ReadHeaderTimeout: 5 * time.Second,
	}

	grpcListener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return fmt.Errorf("listen for worker connections: %w", err)
	}
	grpcServer := grpc.NewServer()
	schedulerv1.RegisterWorkerGatewayServer(grpcServer, grpcapi.NewServer(jobCoordinator))

	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runtimeContext, cancelRuntime := context.WithCancel(shutdownSignal)
	defer cancelRuntime()
	go jobCoordinator.Run(runtimeContext)

	serverError := make(chan error, 2)
	go func() {
		log.Printf("scheduler HTTP API listening on %s", address)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverError <- fmt.Errorf("serve scheduler HTTP API: %w", err)
		}
	}()
	go func() {
		log.Printf("scheduler worker gateway listening on %s", grpcAddress)
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverError <- fmt.Errorf("serve scheduler worker gateway: %w", err)
		}
	}()

	var runError error
	select {
	case runError = <-serverError:
	case <-shutdownSignal.Done():
	}
	cancelRuntime()

	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	grpcServer.Stop()
	if err := httpServer.Shutdown(shutdownContext); err != nil {
		return errors.Join(runError, fmt.Errorf("shutdown scheduler HTTP API: %w", err))
	}

	return runError
}
