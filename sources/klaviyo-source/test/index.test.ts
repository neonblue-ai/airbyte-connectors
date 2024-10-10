import {
  AirbyteLogLevel,
  AirbyteSourceLogger,
  AirbyteSpec,
  SyncMode,
} from 'faros-airbyte-cdk';
import fs from 'fs-extra';
import path from 'path';

import * as sut from '../src/index';
import {KlaviyoStream} from '../src/streams/klaviyo';

const config = require('../src/secrets/config.json');

describe('index', () => {
  const logger = new AirbyteSourceLogger(
    // Shush messages in tests, unless in debug
    process.env.LOG_LEVEL === 'debug'
      ? AirbyteLogLevel.INFO
      : AirbyteLogLevel.FATAL
  );

  // shared between tests
  const source = new sut.KlaviyoSource(logger);

  test('spec', async () => {
    await expect(source.spec()).resolves.toStrictEqual(
      new AirbyteSpec(readResourceFile('spec.json'))
    );
  });

  test('check connection', async () => {
    await expect(source.checkConnection(config)).resolves.toStrictEqual([
      true,
      undefined,
    ]);
  });

  const testStream = async (
    streamName: string,
    expectedData: any
  ): Promise<void> => {
    const stream = source
      .streams(config)
      .find((s) => s.name === streamName) as KlaviyoStream;
    const iter = stream.readRecords(
      SyncMode.INCREMENTAL,
      undefined,
      undefined,
      undefined
    );
    const getUpdatedStateSpy = jest.spyOn(stream, 'getUpdatedState');

    // jest
    //   .spyOn(stream, 'getUpdatedState')
    //   .mockImplementation(
    //     (currentStreamState: any, latestRecord: any, streamSlice?: any) => {
    //       console.log(currentStreamState, latestRecord, streamSlice, 'xxxx');
    //       return {};
    //     }
    //   );

    const items = [];
    for await (const item of iter) {
      items.push(item);
      stream.controller.abort();
      break;
    }

    expect(items).toStrictEqual(expectedData.data);
  };

  test('streams - users', async () => {
    await testStream('profiles', {
      data: [
        {
          id: 'H4m2uf',
          email: 'bethgoldberg151@gmail.com',
          phone_number: '+16302806779',
          external_id: null,
          first_name: 'Beth',
          last_name: 'Goldberg',
          organization: null,
          locale: null,
          title: null,
          image: null,
          created: '2017-07-14T19:35:49.000Z',
          updated: '2024-05-17T02:30:11.000Z',
          last_event_date: '2023-04-01T21:27:33.000Z',
          location: {
            address1: '645 W Chicago Ave.',
            address2: null,
            city: 'Hinsdale',
            country: 'United States',
            latitude: null,
            longitude: null,
            region: 'Illinois',
            zip: '60521',
            timezone: 'America/Chicago',
            ip: '73.204.50.195',
          },
          properties: {
            'Accepts Marketing': true,
            'Shopify Tags': [],
            $neonblue: true,
            $source: -6,
          },
        },
      ],
    });
  }, 1000000);
});

function readResourceFile(fileName: string): any {
  return JSON.parse(
    // this needs to be defined differently due to jest issue
    // https://github.com/jestjs/jest/issues/6155
    fs.readFileSync(path.resolve(__dirname, `../resources/${fileName}`), 'utf8')
  );
}
