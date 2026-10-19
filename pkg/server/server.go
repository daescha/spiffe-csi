// Package server provides the gRPC server for the CSI driver.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spiffe/spiffe-csi/pkg/logkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Config holds the configuration for the gRPC server.
type Config struct {
	Log           logr.Logger
	CSISocketPath string
	// MetricsBindAddress is the TCP address for the Prometheus metrics endpoint. Empty disables it.
	MetricsBindAddress string
	Driver             Driver
}

// Driver is the interface that the CSI driver must implement.
type Driver interface {
	csi.IdentityServer
	csi.NodeServer
}

// Run starts the gRPC server and blocks until it exits.
func Run(config Config) error {
	if config.CSISocketPath == "" {
		return errors.New("CSI socket path is required")
	}

	if err := os.Remove(config.CSISocketPath); err != nil && !os.IsNotExist(err) {
		config.Log.Error(err, "Unable to remove CSI socket")
	}

	listener, err := net.Listen("unix", config.CSISocketPath)
	if err != nil {
		return fmt.Errorf("unable to create CSI socket listener: %w", err)
	}

	rpcLogger := rpcLogger{Log: config.Log}
	unaryInterceptors := []grpc.UnaryServerInterceptor{rpcLogger.UnaryRPCLogger}

	if config.MetricsBindAddress != "" {
		reg := prometheus.NewRegistry()
		reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		unaryInterceptors = append(unaryInterceptors, newRPCMetrics(reg).UnaryRPCMetrics)

		metricsListener, err := net.Listen("tcp", config.MetricsBindAddress)
		if err != nil {
			return fmt.Errorf("unable to create metrics listener: %w", err)
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		metricsServer := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			config.Log.Error(metricsServer.Serve(metricsListener), "Metrics server stopped")
		}()
	}

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.StreamInterceptor(rpcLogger.StreamRPCLogger),
	)
	csi.RegisterIdentityServer(server, config.Driver)
	csi.RegisterNodeServer(server, config.Driver)

	config.Log.Info("Listening...")
	return server.Serve(listener)
}

type rpcLogger struct {
	Log logr.Logger
}

func (l rpcLogger) UnaryRPCLogger(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	log := l.Log.WithValues(logkeys.FullMethod, info.FullMethod)
	resp, err := handler(ctx, req)
	if err != nil {
		log.Error(err, "RPC failed")
	} else {
		log.V(2).Info("RPC succeeded")
	}
	return resp, err
}

func (l rpcLogger) StreamRPCLogger(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	log := l.Log.WithValues(logkeys.FullMethod, info.FullMethod)
	err := handler(srv, ss)
	if err != nil {
		log.Error(err, "RPC failed")
	} else {
		log.V(2).Info("RPC succeeded")
	}
	return err
}

type rpcMetrics struct {
	duration *prometheus.HistogramVec
}

func newRPCMetrics(reg prometheus.Registerer) rpcMetrics {
	m := rpcMetrics{duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "spiffe_csi_driver_rpc_duration_seconds",
		Help: "Duration of CSI RPCs handled by the driver, by method and gRPC status code.",
	}, []string{"method", "code"})}
	reg.MustRegister(m.duration)
	return m
}

func (m rpcMetrics) UnaryRPCMetrics(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	m.duration.WithLabelValues(info.FullMethod, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return resp, err
}
