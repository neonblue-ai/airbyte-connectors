import {
  AirbyteLogLevel,
  AirbyteSourceLogger,
  SyncMode,
} from 'faros-airbyte-cdk';
import fs from 'fs';
import path from 'path';

import {KlaviyoSource} from './index';

(async () => {
  const streamName = process.argv[2];
  const total = Number(process.argv[3] ?? '1');
  const logger = new AirbyteSourceLogger(AirbyteLogLevel.INFO);
  const source = new KlaviyoSource(logger);
  const config = JSON.parse(
    fs.readFileSync(
      path.resolve(__dirname, `../src/secrets/config.json`),
      'utf8'
    )
  );
  const stream = source.streams(config).find((s) => s.name === streamName);

  let n = 0;
  if (stream) {
    for await (const record of stream.readRecords(
      SyncMode.INCREMENTAL,
      undefined,
      undefined,
      {}
    )) {
      console.log(`Record ${n++} / ${total}`, JSON.stringify(record));
      if (total > 0 && n >= total) break;
    }
  }
})();
