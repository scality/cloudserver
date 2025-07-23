# TRACING CONFIGURATION BACKUP - Wed Jul 23 07:45:20 PM CEST 2025

This backup contains all Kubernetes resources related to distributed tracing:

## Components Included:
- **Beyla**: eBPF-based observability (ConfigMaps, DaemonSets)
- **OTEL Collector**: OpenTelemetry data collection and forwarding
- **Tempo**: Distributed tracing backend
- **Prometheus**: Metrics collection and storage
- **Cilium**: CNI with L7 proxy tracing capabilities
- **Hubble**: Network observability for Cilium
- **Miscellaneous**: Network policies, ingress configs, CRDs

## Working Configuration Status:
✅ Connected traces showing: ingress-nginx → cilium-envoy → cloudserver → vault → storage-proxy → mongodb
✅ 24 spans in single trace with proper parent-child relationships
✅ MongoDB operations visible (INSERT/UPDATE)

Generated on: Wed Jul 23 07:45:20 PM CEST 2025
