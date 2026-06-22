import express from 'express';
import { extract } from 'tar-stream';
import pLimit from 'p-limit';
import { createGunzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { createWriteStream, createReadStream } from 'node:fs';
import { unlink, mkdir } from 'node:fs/promises';
import crypto from 'node:crypto';
import path from 'node:path';
import { openAsBlob } from 'node:fs';

const PORT = process.env.PORT || 3000;
const IMMICH_API_URL = process.env.IMMICH_API_URL || 'http://immich-server:2283/api/assets';
const IMMICH_API_KEY = process.env.IMMICH_API_KEY;
const UPLOAD_CONCURRENCY = parseInt(process.env.UPLOAD_CONCURRENCY || '3', 10);
const DEVICE_ID = process.env.DEVICE_ID || 'k8s-takeout-streamer';
const TMP_DIR = process.env.TMP_DIR || '/work/tmp';
const MAX_STAGED_FILES = parseInt(process.env.MAX_STAGED_FILES || '15', 10);
const MAX_STAGED_BYTES = parseInt(process.env.MAX_STAGED_BYTES || (10 * 1024 * 1024 * 1024).toString(), 10);

const app = express();

// Ensure TMP_DIR exists
await mkdir(TMP_DIR, { recursive: true }).catch(() => {});

app.post('/upload-archive', async (req, res) => {
  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Transfer-Encoding', 'chunked');

  const log = (data) => {
    res.write(JSON.stringify(data) + '\n');
  };

  log({ message: 'Starting archive upload processing' });

  const limit = pLimit(UPLOAD_CONCURRENCY);
  const extractor = extract();
  const gunzip = createGunzip();

  let stagedFilesCount = 0;
  let stagedFilesBytes = 0;

  extractor.on('entry', async (header, stream, next) => {
    if (header.type !== 'file') {
      stream.resume();
      return next();
    }

    // Strict Predictive Backpressure
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

    try {
      // 1. Stage sequentially to fast emptyDir to avoid stream consumption bugs
      await pipeline(stream, createWriteStream(tmpPath));

      // 2. Calculate SHA-1 hash for Immich deduplication
      const hash = crypto.createHash('sha1');
      await pipeline(createReadStream(tmpPath), hash);
      const calculatedHash = hash.digest('hex');

      // 3. Queue the upload concurrently
      limit(async () => {
        let attempt = 0;
        const maxAttempts = 3;

        try {
          while (attempt < maxAttempts) {
          attempt++;
          try {
            const fileBlob = await openAsBlob(tmpPath);
            const form = new FormData();
            form.append('assetData', fileBlob, path.basename(header.name));
            form.append('deviceAssetId', calculatedHash);
            form.append('deviceId', DEVICE_ID);
            // Immich expects some extra fields sometimes, but based on docs/request:
            // https://immich.app/docs/api/upload-asset
            // fileReport, assetData, deviceId, deviceAssetId, fileCreatedAt, fileModifiedAt, isFavorite, duration

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
          await unlink(tmpPath).catch(() => {});
          stagedFilesCount--;
          stagedFilesBytes -= fileSize;
        }
      });

      next();
    } catch (err) {
      log({ error: 'Staging failed', name: header.name, detail: err.message });
      await unlink(tmpPath).catch(() => {});
      stagedFilesCount--;
      stagedFilesBytes -= fileSize;
      next();
    }
  });

  try {
    await pipeline(req, gunzip, extractor);
    log({ message: 'Archive fully streamed, waiting for pending uploads to finish' });

    // Wait for all p-limit tasks to finish
    await new Promise((resolve) => {
      const check = () => {
        if (limit.activeCount === 0 && limit.pendingCount === 0) {
          resolve();
        } else {
          setTimeout(check, 100);
        }
      };
      check();
    });

    log({ message: 'Processing finished' });
    res.end();
  } catch (err) {
    log({ error: 'Archive processing failed', detail: err.message });
    res.status(500).end();
  }
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
