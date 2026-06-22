# Immich High-Speed In-Cluster Tar Streaming Service

This service provides a high-performance, memory-efficient way to ingest massive Google Takeout archives into Immich from within a Kubernetes cluster.

## Architecture

- **Sequential Streaming:** Processes incoming `.tar` or `.tar.gz` archives on the fly without storing the entire archive in memory or on disk.
- **Fast Staging:** Individual files are staged to a high-speed, RAM-backed `emptyDir` volume (`/work/tmp`) for SHA-1 hashing and deduplication.
- **Concurrency Control:** Manages multiple concurrent uploads to Immich using `p-limit`.
- **Backpressure:** Safely pauses the incoming stream if the upload queue is full.
- **Native Efficiency:** Built with Node.js 24 using native `fetch`, `FormData`, and `openAsBlob` for zero-copy operations.

## Deployment

### Prerequisites

- A running Immich instance in the same Kubernetes cluster.
- Immich API Key.

### Kustomize

1. Navigate to the `kustomize/` directory.
2. Update `secret.yaml` with your `IMMICH_API_KEY`.
3. Update `deployment.yaml` with the correct image path.
4. Apply the configuration:
   ```bash
   kubectl apply -k .
   ```

## Usage

You can stream a local Google Takeout archive to the service using `curl`:

```bash
curl -X POST \
  -H "Content-Type: application/octet-stream" \
  -T your-takeout-archive.tgz \
  http://immich-takeout-streamer.immich.svc.cluster.local/upload-archive
```

The service will return a chunked JSON response (NDJSON) showing the progress of each file:

```json
{"message":"Starting archive upload processing"}
{"status":"Success","name":"Takeout/Google Photos/Photos from 2023/IMG_1234.jpg","hash":"..."}
{"status":"Graceful Duplicate Skip","name":"Takeout/Google Photos/Photos from 2023/IMG_1235.jpg","hash":"..."}
...
{"message":"Processing finished"}
```

## Configuration

The service can be configured via environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `IMMICH_API_URL` | `http://immich-server:2283/api/assets` | Immich API endpoint |
| `IMMICH_API_KEY` | (Required) | Your Immich API Key |
| `UPLOAD_CONCURRENCY` | `3` | Number of parallel uploads |
| `DEVICE_ID` | `k8s-takeout-streamer` | Device ID reported to Immich |
| `PORT` | `3000` | Port the service listens on |
| `TMP_DIR` | `/work/tmp` | Directory for staging files |
