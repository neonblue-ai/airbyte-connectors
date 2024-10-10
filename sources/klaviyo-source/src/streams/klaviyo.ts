import {AirbyteLogger, AirbyteStreamBase} from 'faros-airbyte-cdk';
import fs from 'fs';
import fsPromises from 'fs/promises';
import _ from 'lodash';
import moment from 'moment';
import pEvent from 'p-event';
import {Transform, TransformCallback, TransformOptions} from 'stream';
import throat from 'throat';
import tmp from 'tmp';
import {z} from 'zod';
import {zodToJsonSchema} from 'zod-to-json-schema';

import {Klaviyo} from '../klaviyo';
import {DatePropertiesToString, KeysToSnakeCase, KlaviyoConfig} from '../types';

tmp.setGracefulCleanup();

export function fromZodType(type: z.AnyZodObject) {
  const schema: any = _.omit(zodToJsonSchema(type), ['additionalProperties']);
  schema.properties = _.mapValues(schema.properties, (v) =>
    _.omit(v, ['additionalProperties'])
  );
  return schema;
}

export function fromApiRecordAttributes<T extends Record<string, any>>(
  record: T
) {
  return _.mapKeys(record, (v, k) => _.snakeCase(k)) as KeysToSnakeCase<
    DatePropertiesToString<T>
  >;
}

export function momentRanges(options: {
  from: moment.Moment;
  to?: moment.Moment;
  step: moment.Duration;
  stepOverlap?: moment.Duration;
  startOverlap?: moment.Duration;
}) {
  const {
    from,
    to = moment.utc(),
    step,
    stepOverlap = moment.duration(0),
    startOverlap = moment.duration(0),
  } = options;
  const ranges: Array<{from: moment.Moment; to: moment.Moment}> = [];
  let isFirst = true;
  for (let d = moment.utc(from.clone()); d.isBefore(to); ) {
    const a = d.clone();
    const b = d.add(step).clone();
    ranges.push({
      from: isFirst ? a.subtract(startOverlap) : a.subtract(stepOverlap),
      to: b.add(stepOverlap),
    });
    isFirst = false;
  }
  return ranges;
}

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
  private writeBufferLimit = 64 * 1024;
  private readBufferLimit = 512 * 1024;

  constructor(
    public generator: AsyncGenerator<T>,
    options: {
      controller: AbortController;
    }
  ) {
    this.controller = options.controller;
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
    this.controller.signal.throwIfAborted();
    if (this.writeWorker) {
      throw new Error('Already started.');
    }
    this.writeWorker = new Promise<void>((resolve, reject) => {
      const file = fs
        .createWriteStream(this.bufferFile, {
          encoding: 'utf-8',
          flags: 'a',
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
        } catch (e: any) {
          if (e.name !== 'AbortError') {
            this.controller.abort();
            reject(e);
          }
        } finally {
          writer.end();
        }
      })();
    });
    return this.writeWorker;
  }

  async *process(): AsyncGenerator<T> {
    this.controller.signal.throwIfAborted();
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
        if (this.isDone && !lastPartial) {
          break;
        }
        await Promise.race([
          pEvent(watcher, ['close', 'change'], {
            rejectionEvents: ['error'],
          }),
          this.writeWorker,
        ]);
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
  private lastIds = new Set<string>();
  private currentIds = new Set<string>();

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

  /**
   * The primary key of the stream (restricted to non-nested values for now).
   */
  abstract get primaryKey(): string;

  /**
   * The cursor field of the stream (restricted to non-nested values for now).
   */
  abstract get cursorField(): string | never[];

  async *parallelSequentialRead<
    T extends {from?: moment.Moment; to?: moment.Moment},
    U extends Record<string, any>,
    V extends this,
  >(
    options: {
      parallel?: number;
      dedupe?: boolean;
    },
    args: T[],
    generatorFn: (this: V, args: T) => AsyncGenerator<U>
  ) {
    const {parallel = 10} = options;
    const cursorField = Array.isArray(this.cursorField)
      ? undefined
      : this.cursorField;
    const dedupe = options.dedupe && !!this.primaryKey;

    // setup strems
    const streams = args.map(
      (params) =>
        new FileBufferedProcessor(generatorFn.call(this as V, params), {
          controller: this.controller,
        })
    );

    // start all the streams
    streams.map(
      throat(parallel, async (stream) => {
        try {
          this.controller.signal.throwIfAborted();
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
      for (let stream = streams.shift(); stream; stream = streams.shift()) {
        this.controller.signal.throwIfAborted();
        for await (const item of stream.process()) {
          this.controller.signal.throwIfAborted();
          if (dedupe) {
            // TODO: narrow the number of current ids stored by examining the chunk's from field
            this.currentIds.add(item[this.primaryKey]);
            if (this.lastIds.has(item[this.primaryKey])) {
              continue;
            }
          }
          yield item;
        }
        await stream.cleanup();
        // reset the currentIds
        this.lastIds = this.currentIds;
        this.currentIds = new Set<string>();
      }
    } catch (e: any) {
      if (e.name !== 'AbortError') {
        this.controller.abort();
        throw e;
      }
    }
  }
}
