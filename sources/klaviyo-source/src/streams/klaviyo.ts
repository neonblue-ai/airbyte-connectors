import {AirbyteLogger, AirbyteStreamBase} from 'faros-airbyte-cdk';
import fs from 'fs';
import fsPromises from 'fs/promises';
import {Transform, TransformCallback, TransformOptions} from 'stream';
import throat from 'throat';
import tmp from 'tmp';

import {Klaviyo} from '../klaviyo';
import {KlaviyoConfig} from '../types';

class BufferedJsonTransform extends Transform {
  private buffer: string = '';
  private bufferSize: number;

  constructor(options: TransformOptions & {bufferSize?: number} = {}) {
    const {bufferSize, ...opts} = options;
    super({
      ...opts,
      encoding: 'utf-8',
      objectMode: true,
    });
    this.bufferSize = bufferSize ?? 0;
  }

  _transform(chunk: any, encoding: string, next: TransformCallback) {
    const stringified = JSON.stringify(chunk);
    this.buffer += stringified;
    this.buffer += '\n';
    if (this.buffer.length >= this.bufferSize) {
      this.push(this.buffer);
      this.buffer = '';
    }
    next();
  }

  _flush(next: TransformCallback) {
    if (this.buffer.length > 0) {
      this.push(this.buffer);
    }
    next();
  }
}

class FileBufferedProcessor<T> {
  controller: AbortController;
  bufferFile = tmp.tmpNameSync({postfix: '.json'});

  isDone = false;
  isProcessing = false;

  private writeWorker: Promise<void> | undefined;
  private writeBufferLimit = 128 * 1024;
  private readBufferLimit = 1024 * 1024;

  constructor(
    public generator: AsyncGenerator<T>,
    options: {
      controller?: AbortController;
    } = {}
  ) {
    this.controller = options.controller ?? new AbortController();
  }

  get worker() {
    return this.writeWorker;
  }

  /**
   * Gets a unique file descriptor for reading from the buffer file.
   */
  async getReadFd() {
    try {
      await fsPromises.access(this.bufferFile, fs.constants.F_OK);
    } catch {
      await fsPromises.writeFile(this.bufferFile, '');
    }
    return await fsPromises.open(this.bufferFile, 'r');
  }

  async start() {
    if (this.writeWorker) {
      throw new Error('Already started.');
    }
    this.writeWorker = new Promise<void>((resolve, reject) => {
      const file = fs
        .createWriteStream(this.bufferFile, {
          encoding: 'utf-8',
        })
        .on('error', reject)
        .on('close', () => {
          this.writeWorker = undefined;
          this.isDone = true;
          resolve();
        });
      const writer = new BufferedJsonTransform({
        bufferSize: this.writeBufferLimit,
      });
      writer.pipe(file);
      // start the generator in a separate async function to avoid blocking the main thread
      (async () => {
        try {
          for await (const value of this.generator) {
            this.controller.signal.throwIfAborted();
            writer.write(value);
          }
        } catch (err) {
          reject(err);
        } finally {
          writer.end();
        }
      })();
    });
    return this.writeWorker;
  }

  async *process(): AsyncGenerator<T> {
    if (this.isProcessing) {
      throw new Error('Already processing.');
    }
    this.isProcessing = true;

    // get read handle
    const fd = await this.getReadFd();

    // setup file watcher
    const watcher = fs.watch(this.bufferFile, {
      persistent: false,
      signal: this.controller.signal,
    });
    const waitForChange = () => {
      return new Promise<void>((resolve, reject) => {
        const change = () => {
          watcher.off('error', abort);
          watcher.off('close', change);
          watcher.off('change', change);
          resolve();
        };
        const abort = (err: Error) => {
          watcher.off('error', abort);
          watcher.off('close', change);
          watcher.off('change', change);
          reject(err);
        };
        watcher.on('change', change).on('error', abort).on('close', change);
      });
    };

    // stream from file buffer
    let lastPartial = '';
    try {
      while (true) {
        this.controller.signal.throwIfAborted();
        let chunk: fs.promises.FileReadResult<Buffer>;
        do {
          this.controller.signal.throwIfAborted();
          chunk = await fd.read({
            buffer: Buffer.alloc(this.readBufferLimit),
          });
          if (chunk.bytesRead > 0) {
            const lines: string[] = chunk.buffer
              .toString('utf-8', 0, chunk.bytesRead)
              .split('\n');
            const nextPartial = lines.pop() ?? '';
            for (let i = 0; i < lines.length; i++) {
              this.controller.signal.throwIfAborted();
              const line = lines[i];
              if (i === 0) {
                // prepend the partial line from the previous chunk
                yield JSON.parse(lastPartial + line);
              } else {
                // yield the line as is
                yield JSON.parse(line);
              }
            }
            lastPartial = nextPartial;
          }
        } while (chunk.bytesRead > 0);
        if (this.isDone) {
          break;
        }
        await Promise.race([waitForChange(), this.writeWorker]);
      }
    } catch (err: any) {
      if (err.name !== 'AbortError') {
        throw err;
      }
    } finally {
      await fd.close();
      watcher.close();
    }
  }

  async cleanup() {
    await fsPromises.rm(this.bufferFile, {force: true});
  }
}

export abstract class KlaviyoStream extends AirbyteStreamBase {
  public readonly controller: AbortController = new AbortController();

  constructor(
    logger: AirbyteLogger,
    readonly config: KlaviyoConfig,
    readonly client: Klaviyo
  ) {
    super(logger);
    logger.info(
      `Initialized ${this.name} stream with config: ${JSON.stringify(config)}`
    );
  }

  async *parallelSequentialRead<T, U, V extends this>(
    options: {
      parallel?: number;
    },
    args: T[],
    generatorFn: (this: V, args: T) => AsyncGenerator<U>
  ) {
    const {parallel = 10} = options;

    // setup strems
    const streams = args.map(
      (params) =>
        new FileBufferedProcessor(generatorFn.call(this as V, params), {
          controller: this.controller,
        })
    );

    // start all the streams
    const workers = streams.map(
      throat(parallel, async (stream) => {
        try {
          await stream.start();
        } catch (e: any) {
          if (e.name !== 'AbortError') {
            this.controller.abort();
            throw e;
          }
        }
      })
    );

    // stream sequentially
    try {
      for (const stream of streams) {
        this.controller.signal.throwIfAborted();
        for await (const item of stream.process()) {
          this.controller.signal.throwIfAborted();
          yield item;
        }
      }
    } catch (e: any) {
      if (e.name !== 'AbortError') {
        this.controller.abort();
        throw e;
      }
    }

    // should be a no-op but just in case
    await Promise.all(workers);
    await Promise.all(streams.map((s) => s.cleanup()));
  }
}
