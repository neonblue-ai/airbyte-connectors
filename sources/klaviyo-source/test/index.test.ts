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
    expectedState: any
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
      _.pick(JSON.parse(JSON.stringify(items[0])), _.keys(expectedItem))
    ).toStrictEqual(expectedItem);
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
        account_id: 'M8RPxA',
      },
      {
        cutoff: 1500060873000,
      }
    );
  }, 1000000);

  test('streams - campaigns', async () => {
    await testStream(
      'campaigns',
      {
        id: 'LXgqe8',
        channel: 'email',
        name: 'BLD Review Request',
        status: 'Sent',
        archived: false,
        audiences: {
          included: ['QrsPkW'],
          excluded: [],
        },
        send_options: {
          useSmartSending: true,
        },
        tracking_options: {
          isAddUtm: true,
          utmParams: [],
          isTrackingClicks: true,
          isTrackingOpens: true,
        },
        send_strategy: {
          method: 'static',
          optionsStatic: {
            datetime: '2017-09-19T06:04:19.000Z',
            isLocal: false,
            sendPastRecipientsImmediately: null,
          },
          optionsThrottled: null,
          optionsSto: null,
        },
        created_at: '2017-09-19T00:08:16.000Z',
        scheduled_at: '2017-09-19T06:04:19.000Z',
        updated_at: '2017-09-19T06:04:22.000Z',
        send_time: '2017-09-19T06:04:19.000Z',
        campaign_message_ids: ['01GF857XGP8CFZQS19XSPGZC5V'],
        account_id: 'M8RPxA',
      },
      {
        cutoff: 1505779696000,
      }
    );
  }, 1000000);

  test('streams - events', async () => {
    await testStream(
      'events',
      {
        id: '3b5kgp62',
        timestamp: 1494464965,
        event_properties: {
          tags: [],
          Items: ['BLD Product  : 3328537958'],
          'Item Count': 1,
          'Source Name': 'web',
          Collections: [],
          $event_id: '4815467080',
          $value: 1392.11,
          $extra: {
            subtotal_price: '1270.91',
            buyer_accepts_marketing: false,
            reference: null,
            shipping_lines: [
              {
                code: 'Standard Shipping',
                tax_lines: [],
                title: 'Standard Shipping',
                price: '10.00',
                id: 4012173960,
                phone: null,
                requested_fulfillment_service_id: null,
                carrier_identifier: null,
                source: 'shopify',
                delivery_category: null,
              },
            ],
            cart_token: 'fdea1c8a6e88b07a70f8a03fd1038e94',
            updated_at: '2017-07-01T11:40:36-07:00',
            taxes_included: false,
            currency: 'USD',
            source_identifier: null,
            source_name: 'web',
            closed_at: '2017-07-01T11:40:36-07:00',
            processed_at: '2017-05-10T18:09:25-07:00',
            payment_gateway_names: ['Custom'],
            location_id: null,
            id: 4815467080,
            confirmed: true,
            user_id: null,
            fulfillments: [],
            full_landing_site: 'http://letsbld-com.myshopify.com/password',
            tags: '',
            total_price_usd: '1392.11',
            financial_status: 'pending',
            gateway: 'Custom',
            note: null,
            landing_site: '/password',
            processing_method: 'manual',
            total_line_items_price: '1270.91',
            cancelled_at: null,
            test: false,
            email: 'indralw@ejago.com',
            total_tax: '111.20',
            billing_address: {
              province: 'California',
              city: 'Los ANgeles',
              first_name: 'indra Test',
              last_name: 'Widjaja',
              name: 'indra Test Widjaja',
              zip: '90024',
              province_code: 'CA',
              address1: '2323',
              address2: '',
              longitude: -118.4301112,
              phone: null,
              country_code: 'US',
              country: 'United States',
              latitude: 34.0417175,
              company: null,
            },
            cancel_reason: null,
            tax_lines: [
              {
                price: '92.14',
                rate: 0.0725,
                title: 'CA State Tax',
              },
              {
                price: '19.06',
                rate: 0.015,
                title: 'Los Angeles County Tax',
              },
            ],
            landing_site_ref: null,
            source_url: null,
            number: 1,
            phone: null,
            total_discounts: '0.00',
            discount_codes: [],
            checkout_id: 14024599752,
            browser_ip: null,
            device_id: null,
            customer: {
              total_spent: '0.00',
              multipass_identifier: null,
              first_name: 'indra Test',
              last_name: 'Widjaja',
              orders_count: 42,
              created_at: '2017-05-10T16:54:39-07:00',
              tags: '',
              updated_at: '2017-07-01T07:52:56-07:00',
              last_order_id: 4898209416,
              id: 5181474824,
              note: null,
              phone: null,
              state: 'enabled',
              default_address: {
                province: 'California',
                city: 'Los ANgeles',
                first_name: 'Indra',
                last_name: 'Widjaja',
                name: 'Indra Widjaja',
                zip: '90024',
                province_code: 'CA',
                default: true,
                address1: 'sadsad',
                address2: '',
                id: 5570513544,
                phone: null,
                country_code: 'US',
                country: 'United States',
                country_name: 'United States',
                customer_id: 5181474824,
                company: null,
              },
              tax_exempt: false,
              accepts_marketing: false,
              email: 'indralw@ejago.com',
              last_order_name: '#1063',
              verified_email: true,
            },
            line_items: [
              {
                requires_shipping: true,
                pre_tax_price: '1270.91',
                variant_id: 35899065608,
                id: 9231926856,
                product_exists: true,
                sku: '3328537958',
                line_price: 1270.91,
                title: 'BLD Product  : 3328537958',
                origin_location: {
                  province_code: 'CA',
                  name: 'NZXT, Inc.',
                  zip: '91746',
                  city: 'City of Industry',
                  address1: '13164 Temple Ave.',
                  address2: '',
                  country_code: 'US',
                  id: 2907696072,
                },
                fulfillment_service: 'manual',
                total_discount: '0.00',
                variant_title: '',
                product: {
                  handle: 'bld-product-3328537959',
                  tags: '',
                  variant: {
                    sku: '3328537958',
                    images: [],
                    options: {
                      Title: 'Default Title',
                    },
                    title: 'Default Title',
                  },
                  body_html: '',
                  images: [
                    {
                      src: 'https://cdn.shopify.com/s/files/1/1986/9305/products/case-small_738e3bc9-a46b-4b2d-a4b5-8eab95793936.png?v=1494464926',
                      position: 1,
                    },
                  ],
                  vendor: 'NZXT',
                  properties: {},
                  variant_options: {
                    Title: 'Default Title',
                  },
                  product_type: 'BLD',
                  title: 'BLD Product  : 3328537958',
                  id: 9566367432,
                },
                vendor: 'NZXT',
                tax_lines: [
                  {
                    price: '92.14',
                    rate: 0.0725,
                    title: 'CA State Tax',
                  },
                  {
                    price: '19.06',
                    rate: 0.015,
                    title: 'Los Angeles County Tax',
                  },
                ],
                price: 1270.91,
                taxable: true,
                properties: [],
                product_id: 9566367432,
                fulfillable_quantity: 1,
                name: 'BLD Product  : 3328537958',
                gift_card: false,
                fulfillment_status: null,
                variant_inventory_management: null,
                grams: 0,
                destination_location: {
                  province_code: 'CA',
                  name: 'indra Test Widjaja',
                  zip: '90024',
                  city: 'Los ANgeles',
                  address1: '2323',
                  address2: '',
                  country_code: 'US',
                  id: 2907842824,
                },
                quantity: 1,
              },
            ],
            total_price: '1392.11',
            name: '#1001',
            refunds: [],
            checkout_token: '0faf0c527598c5f4ffe52ecc1dc2a2a2',
            client_details: {
              browser_width: 1280,
              user_agent:
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
              accept_language: 'en-US,en;q=0.8,id;q=0.6,ms;q=0.4,la;q=0.2',
              browser_height: 635,
              session_hash: '03aafe5555baab225a7511f95913ab64',
              browser_ip: '180.254.96.79',
            },
            created_at: '2017-05-10T18:09:25-07:00',
            note_attributes: [],
            fulfillment_status: null,
            customer_locale: null,
            referring_site: '',
            shipping_address: {
              province: 'California',
              city: 'Los ANgeles',
              first_name: 'indra Test',
              last_name: 'Widjaja',
              name: 'indra Test Widjaja',
              zip: '90024',
              province_code: 'CA',
              address1: '2323',
              address2: '',
              longitude: -118.4301112,
              phone: null,
              country_code: 'US',
              country: 'United States',
              latitude: 34.0417175,
              company: null,
            },
            contact_email: 'indralw@ejago.com',
            order_status_url:
              'https://checkout.shopify.com/19869305/checkouts/0faf0c527598c5f4ffe52ecc1dc2a2a2/thank_you_token?key=5491201a09db8856572b72d5f017e391',
            order_number: 1001,
            token: 'e09a360e81788dc03495db08c6d49a07',
            total_weight: 0,
          },
        },
        datetime: '2017-05-11T01:09:25.000Z',
        uuid: '792f9080-35e6-11e7-8001-fdfd74fd64f6',
        metric_id: 'Hb82XC',
        profile_id: 'Qex4z5',
        account_id: 'M8RPxA',
      },
      {
        cutoff: 1494464965000,
      }
    );
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
});

function readResourceFile(fileName: string): any {
  return JSON.parse(
    // this needs to be defined differently due to jest issue
    // https://github.com/jestjs/jest/issues/6155
    fs.readFileSync(path.resolve(__dirname, `../resources/${fileName}`), 'utf8')
  );
}
