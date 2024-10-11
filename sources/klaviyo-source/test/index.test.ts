import {
  AirbyteLogLevel,
  AirbyteSourceLogger,
  AirbyteSpec,
  SyncMode,
} from 'faros-airbyte-cdk';
import fs from 'fs-extra';
import _ from 'lodash';
import path from 'path';

import * as sut from '../src/index';
import {KlaviyoStream} from '../src/streams/klaviyo';

const config = require('../src/secrets/config.json');

describe('index', () => {
  const logger = new AirbyteSourceLogger();
  // Shush messages in tests, unless in debug
  // process.env.LOG_LEVEL === 'debug'
  //   ? AirbyteLogLevel.INFO
  //   : AirbyteLogLevel.FATAL

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
    expectedItem: any,
    expectedState: any,
    omitFields: string[] = []
  ): Promise<void> => {
    const stream = source
      .streams(config)
      .find((s) => s.name === streamName) as KlaviyoStream;

    const items = [];
    const states = [];
    for await (const item of stream.readRecords(
      SyncMode.INCREMENTAL,
      undefined,
      undefined,
      undefined
    )) {
      console.log(`Received item:`, JSON.stringify(item, null, 2));
      items.push(item);
      states.push(stream.getUpdatedState(undefined, item));
      stream.controller.abort();
      break;
    }

    expect(
      _.omit(JSON.parse(JSON.stringify(items[0])), omitFields)
    ).toStrictEqual(_.omit(expectedItem, omitFields));
    expect(states[0]).toStrictEqual(expectedState);
  };

  test('streams - profiles', async () => {
    await testStream(
      'profiles',
      {
        id: 'K3kEKG',
        email: 'designer@nzxt.com',
        phone_number: '+16266365031',
        external_id: 'dd40e1af-33d7-4adf-a5e4-678d4784179a',
        first_name: 'Johnny',
        last_name: 'Hou',
        organization: null,
        locale: null,
        title: null,
        image: null,
        created: '2017-07-14T19:34:33.000Z',
        updated: '2024-10-10T22:57:58.000Z',
        last_event_date: '2024-10-10T22:57:56.000Z',
        location: {
          address_1: '1380 S LOS ROBLES AVE',
          address_2: null,
          city: 'PASADENA',
          country: 'United States',
          latitude: 34.0544,
          longitude: -118.244,
          region: 'California',
          zip: '91106',
          timezone: 'America/Los_Angeles',
          ip: '66.249.92.33',
        },
        properties: {
          please_choose_from_below_a_category_that_best_fits_your_request:
            'Hardware Product',
          shopify_tags: [],
          describe_your_request_below:
            '1. List Amazon categories we should be tracking re: https://app.mode.com/nzxt/reports/87b7b5464c2d \n2. Amazon Best Sellers: Computer Case Fans\n3. Amazon Best Sellers - Computer Motherboards\n4. Amazon Best Sellers: Best Computer Speakers\n5. Amazon Best Sellers: Best PC Game Headsets \n6. Amazon Best Sellers: Best PlayStation 4 Headsets\n7. Amazon Best Sellers: Water Cooling Systems\n8. Amazon Best Sellers: Computer Monitors\n9. Amazon Best Sellers: PC Accessories\n10. Amazon Best Sellers: Computer Microphones (also includes boom bundles)\n11. Amazon Best Sellers: PC Gaming Keyboards\n12. Amazon Best Sellers: PC Gaming Mice\n13. Amazon Best Sellers: Internal TV Tuner & Video Capture Cards\n14. Amazon Best Sellers: Continuous Output Lighting\n15. Amazon Best Sellers: Webcams',
          score: 0,
          source: -6,
          expected_date_of_next_order: '11/20/2024',
          accepts_marketing: true,
          username: 'seininja',
          give_your_request_a_short_but_descriptive_title:
            'Need to scrape more Amazon category pages',
          consent: ['email'],
          consent_timestamp: '2024-02-23T23:41:12.985Z',
          what_area_does_your_request_support: 'Hardware Product',
          test_group: 'A_cart-email',
          neonblue: true,
          current_rfm_group: 'Loyal',
          previous_rfm_group: 'Unknown',
        },
        account_id: 'M8RPxA',
      },
      {
        cutoff: 1500060873000,
      },
      ['properties', 'last_event_date', 'updated']
    );
  }, 1000000);

  test('streams - campaigns', async () => {
    await testStream('campaigns', require('../test_files/campaign.json'), {
      cutoff: 1505801062000,
    });
  }, 10000000);

  test('streams - events', async () => {
    await testStream('events', require('../test_files/event.json'), {
      cutoff: 1494464965000,
    });
  }, 1000000);

  test('streams - metrics', async () => {
    await testStream(
      'metrics',
      {
        id: 'HHYjhK',
        name: 'Clicked Email',
        created: '2017-07-14T19:35:45+00:00',
        updated: '2017-07-14T19:35:45+00:00',
        integration: {
          object: 'integration',
          id: '0rG4eQ',
          key: 'klaviyo',
          name: 'Klaviyo',
          category: 'Internal',
        },
        account_id: 'M8RPxA',
      },
      {
        cutoff: 1500060945000,
      }
    );
  }, 1000000);

  test('streams - templates', async () => {
    await testStream('templates', require('../test_files/template.json'), {
      cutoff: 1500060890000,
    });
  }, 1000000);

  test('streams - flows', async () => {
    await testStream('flows', require('../test_files/flow.json'), {
      cutoff: 1500060900000,
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
