package server

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryRPCMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newRPCMetrics(reg)
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Node/NodePublishVolume"}

	_, err := m.UnaryRPCMetrics(context.Background(), nil, info, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.Internal, "boom")
	})
	require.Error(t, err)
	_, err = m.UnaryRPCMetrics(context.Background(), nil, info, func(context.Context, any) (any, error) {
		return "ok", nil
	})
	require.NoError(t, err)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 1)
	require.Equal(t, "spiffe_csi_driver_rpc_duration_seconds", mfs[0].GetName())
	got := map[string]uint64{}
	for _, metric := range mfs[0].GetMetric() {
		labels := map[string]string{}
		for _, l := range metric.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		got[labels["method"]+" "+labels["code"]] = metric.GetHistogram().GetSampleCount()
	}
	require.Equal(t, map[string]uint64{
		"/csi.v1.Node/NodePublishVolume Internal": 1,
		"/csi.v1.Node/NodePublishVolume OK":       1,
	}, got)
}
