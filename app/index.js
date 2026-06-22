import { extract } from 'tar-stream';
import pLimit from 'p-limit';
import path from 'node:path';
import { mkdir } from 'node:fs/promises';
import { Readable } from 'node:stream';

const PORT = process.env.PORT || 3000;
const IMMICH_API_URL = process.env.IMMICH_API_URL || 'http://immich-server:2283/api/assets';
const IMMICH_API_KEY = process.env.IMMICH_API_KEY;
const UPLOAD_CONCURRENCY = parseInt(process.env.UPLOAD_CONCURRENCY || '3', 10);
const DEVICE_ID = process.env.DEVICE_ID || 'k8s-takeout-streamer';
const TMP_DIR = process.env.TMP_DIR || '/work/tmp';
const MAX_STAGED_FILES = parseInt(process.env.MAX_STAGED_FILES || '15', 10);
const MAX_STAGED_BYTES = parseInt(process.env.MAX_STAGED_BYTES || (10 * 1024 * 1024 * 1024).toString(), 10);

if (MAX_STAGED_BYTES > 512 * 1024 * 1024) {
  console.warn(`WARNING: MAX_STAGED_BYTES (${MAX_STAGED_BYTES}) is larger than the default Kubernetes emptyDir limit (512Mi). Ensure your deployment has sufficient resources.`);
}

// Ensure TMP_DIR exists
await mkdir(TMP_DIR, { recursive: true }).catch(() => {});

let stagedFilesCount = 0;
let stagedFilesBytes = 0;

async function handleUploadArchive(req) {
  if (!req.body) {
    return new Response(JSON.stringify({ error: "No body provided" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
  }

  const limit = pLimit(UPLOAD_CONCURRENCY);

  const stream = new ReadableStream({
    async start(controller) {
      const log = (data) => {
        controller.enqueue(new TextEncoder().encode(JSON.stringify(data) + '\n'));
      };

      log({ message: 'Starting archive upload processing (Bun Edition)' });

      const decompressedStream = req.body.pipeThrough(new DecompressionStream("gzip"));
      const tarReader = Readable.fromWeb(decompressedStream);
      const extractor = extract();

      extractor.on('entry', async (header, entryStream, next) => {
        if (header.type !== 'file') {
          entryStream.resume();
          return next();
        }

        const fileSize = header.size || 0;

        const shouldWait = () => {
          return stagedFilesCount >= MAX_STAGED_FILES ||
                 (stagedFilesBytes + fileSize) > MAX_STAGED_BYTES ||
                 limit.pendingCount >= UPLOAD_CONCURRENCY * 2;
        };

        if (shouldWait()) {
          log({
            message: 'Backpressure: waiting for resources',
            stagedCount: stagedFilesCount,
            stagedBytes: stagedFilesBytes,
            nextFileSize: fileSize,
            pendingUploads: limit.pendingCount
          });

          while (shouldWait()) {
            await new Promise(resolve => setTimeout(resolve, 500));
          }
        }

        // Commit to staging this file
        stagedFilesCount++;
        stagedFilesBytes += fileSize;

        const fileExtension = path.extname(header.name);
        const tmpFileName = `${crypto.randomUUID()}${fileExtension}`;
        const tmpPath = path.join(TMP_DIR, tmpFileName);
        const fileHandle = Bun.file(tmpPath);

        try {
          // 1. Stage sequentially to fast emptyDir to avoid stream consumption bugs
          // Using Bun.write(tmpPath, entryStream) which supports ReadableStream/Node Stream
          await Bun.write(tmpPath, entryStream);

          // 2. Calculate SHA-1 hash for Immich deduplication using streaming hasher to avoid OOM
          const hasher = new Bun.CryptoHasher("sha1");
          const fileStream = fileHandle.stream();
          for await (const chunk of fileStream) {
            hasher.update(chunk);
          }
          const calculatedHash = hasher.digest("hex");

          // 3. Queue the upload concurrently
          limit(async () => {
            let attempt = 0;
            const maxAttempts = 3;

            try {
              while (attempt < maxAttempts) {
                attempt++;
                try {
                  const form = new FormData();
                  // Bun.file() returns a Blob-like object which fetch/FormData can handle efficiently
                  form.append('assetData', fileHandle, path.basename(header.name));
                  form.append('deviceAssetId', calculatedHash);
                  form.append('deviceId', DEVICE_ID);

                  const response = await fetch(IMMICH_API_URL, {
                    method: 'POST',
                    headers: {
                      'x-api-key': IMMICH_API_KEY,
                      'Accept': 'application/json',
                    },
                    body: form,
                  });

                  if (response.ok) {
                    log({ status: 'Success', name: header.name, hash: calculatedHash });
                    break;
                  } else if (response.status === 409) {
                    log({ status: 'Graceful Duplicate Skip', name: header.name, hash: calculatedHash });
                    break;
                  } else {
                    const errorText = await response.text();
                    throw new Error(`HTTP ${response.status}: ${errorText}`);
                  }
                } catch (uploadErr) {
                  if (attempt === maxAttempts) {
                    log({ error: 'Upload failed after retries', name: header.name, detail: uploadErr.message });
                  } else {
                    const delay = Math.pow(2, attempt) * 1000;
                    log({ message: 'Retrying upload', name: header.name, attempt, delay });
                    await new Promise(resolve => setTimeout(resolve, delay));
                  }
                }
              }
            } finally {
              // ALWAYS clean up to prevent emptyDir exhaustion
              await fileHandle.delete().catch(() => {});
              stagedFilesCount--;
              stagedFilesBytes -= fileSize;
            }
          });

          next();
        } catch (err) {
          log({ error: 'Staging failed', name: header.name, detail: err.message });
          await fileHandle.delete().catch(() => {});
          stagedFilesCount--;
          stagedFilesBytes -= fileSize;
          next();
        }
      });

      try {
        await new Promise((resolve, reject) => {
          tarReader.pipe(extractor);
          extractor.on('finish', resolve);
          extractor.on('error', reject);
          tarReader.on('error', reject);
        });

        log({ message: 'Archive fully streamed, waiting for pending uploads to finish' });

        // Wait for all p-limit tasks to finish
        while (limit.activeCount > 0 || limit.pendingCount > 0) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }

        log({ message: 'Processing finished' });
        controller.close();
      } catch (err) {
        log({ error: 'Archive processing failed', detail: err.message });
        controller.error(err);
      }
    }
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'application/x-ndjson',
      'Transfer-Encoding': 'chunked',
    }
  });
}

Bun.serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);
    if (req.method === 'POST' && url.pathname === '/upload-archive') {
      return handleUploadArchive(req);
    }
    return new Response("Not Found", { status: 404 });
  },
  error(error) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  },
});

console.log(`Bun server listening on port ${PORT}`);
