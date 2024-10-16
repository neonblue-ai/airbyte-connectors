import {
  AirbyteLogLevel,
  AirbyteSourceLogger,
  SyncMode,
} from 'faros-airbyte-cdk';
import fs from 'fs';
import path from 'path';

import {KlaviyoSource} from './index';
import {KlaviyoStream} from './streams/klaviyo';

(async () => {
  const streamName = process.argv[2];
  const state = process.argv[3] ? JSON.parse(process.argv[3]) : {};
  const total = Number(process.argv[4] ?? '0');
  console.log({streamName, state, total});

  const logger = new AirbyteSourceLogger(AirbyteLogLevel.INFO);
  const source = new KlaviyoSource(logger);
  const config = JSON.parse(
    fs.readFileSync(
      path.resolve(__dirname, `../src/secrets/config.json`),
      'utf8'
    )
  );
  const stream: KlaviyoStream = source
    .streams(config)
    .find((s) => s.name === streamName) as any;

  let n = 0;
  if (stream) {
    for await (const record of stream.readRecords(
      SyncMode.INCREMENTAL,
      undefined,
      undefined,
      state
    )) {
      if (n % 10000 === 0) {
        console.log(`Record ${n} / ${total}`, JSON.stringify(record));
      }
      if (total > 0 && n >= total) {
        console.log(`ABORTING: Record ${n} / ${total}`);
        stream.controller.abort();
      }
      n++;
    }
  }
})();
