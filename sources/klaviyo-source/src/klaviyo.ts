import {URL} from 'node:url';

import {AxiosError} from 'axios';
import Bottleneck from 'bottleneck';
import {backOff} from 'exponential-backoff';
import {
  AccountsApi,
  CampaignsApi,
  CollectionLinks,
  CreatedTokens,
  EventsApi,
  FlowsApi,
  KlaviyoErrorName,
  KlaviyoTokenError,
  ListsApi,
  MetricsApi,
  OAuthApi,
  OAuthSession,
  ProfilesApi,
  RetrievedTokens,
  SegmentsApi,
  Session,
  TemplatesApi,
  TokenStorage,
} from 'klaviyo-api';
import _ from 'lodash';
import moment, {Moment} from 'moment';
import throat from 'throat';

type KlaviyoLimiter = Record<
  keyof typeof KLAVIYO_ENDPOINTS,
  <T>(fn: () => Promise<T>) => Promise<T>
>;

export async function fromGeneratorToArray<T>(
  gen: AsyncIterable<T[]>
): Promise<T[]> {
  const out: T[] = [];
  for await (const x of gen) {
    out.push(...x);
  }
  return out;
}

function getLimiter(): KlaviyoLimiter {
  return Object.fromEntries(
    Object.entries(KLAVIYO_ENDPOINTS).map(([key, value]) => {
      const limiter = new Bottleneck({
        reservoir: value.steady,
        reservoirRefreshAmount: value.steady,
        reservoirRefreshInterval: 60 * 1000,
        minTime: (1000 / value.burst) * 1.25,
        maxConcurrent: 20,
      });
      return [
        key,
        <T>(fn: () => Promise<T>) => {
          return limiter.schedule(fn);
        },
      ];
    })
  ) as any;
}

export class Klaviyo {
  private accountId: string | undefined;
  private limiters: KlaviyoLimiter = getLimiter();

  constructor(private session: Session) {}

  async withRetry<T>(fn: () => Promise<T>) {
    return backOff(fn, {
      maxDelay: 2 * 60 * 1000,
      numOfAttempts: 100,
      startingDelay: 30 * 1000,
      timeMultiple: 2,
      retry(e: AxiosError) {
        return e.response?.status !== 400;
      },
    });
  }

  async withLimiter<T>(
    endpoint: keyof typeof KLAVIYO_ENDPOINTS,
    fn: () => Promise<T>
  ) {
    return this.withRetry(() => this.limiters[endpoint](fn));
  }

  toDatetimeFilterString(d: Moment | number | Date) {
    const m = moment.isMoment(d) ? d.clone().utc() : moment.utc(d);
    return m.format('YYYY-MM-DDTHH:mm:ss') + 'Z';
  }

  private async *fetchAll<P>(
    endpoint: keyof typeof KLAVIYO_ENDPOINTS,
    fn: (
      cursor?: string
    ) => Promise<{body: {links?: CollectionLinks; data: P[]}}>
  ) {
    let next: string | undefined = undefined;
    do {
      const result = await this.withRetry(() =>
        this.limiters[endpoint](() => fn(next))
      );
      yield result.body.data;
      const nextUrl = result.body.links?.next;
      if (nextUrl) {
        const url = new URL(nextUrl);
        next = url.searchParams.get(`page[cursor]`) || undefined;
      } else {
        next = undefined;
      }
    } while (next);
  }

  async *getProfiles(options: Parameters<ProfilesApi['getProfiles']>[0] = {}) {
    const params = {
      pageSize: 100,
      ...options,
    };
    const api = new ProfilesApi(this.session);
    yield* this.fetchAll('GET:/profiles/', (pageCursor) =>
      api.getProfiles({...params, pageCursor})
    );
  }

  async *getEvents(options: Parameters<EventsApi['getEvents']>[0] = {}) {
    const params = {
      ...options,
    };
    const api = new EventsApi(this.session);
    yield* this.fetchAll('GET:/events/', (pageCursor) =>
      api.getEvents({...params, pageCursor})
    );
  }

  async getMetrics() {
    const api = new MetricsApi(this.session);
    return fromGeneratorToArray(
      this.fetchAll('GET:/metrics/', (pageCursor) =>
        api.getMetrics({pageCursor})
      )
    );
  }

  async *getCampaigns(
    filter: Parameters<CampaignsApi['getCampaigns']>[0],
    options: Parameters<CampaignsApi['getCampaigns']>[1] = {}
  ) {
    const params = {
      ...options,
    };
    const api = new CampaignsApi(this.session);
    yield* this.fetchAll('GET:/campaigns/', (pageCursor) =>
      api.getCampaigns(filter, {...params, pageCursor})
    );
  }

  async *getFlows(options: Parameters<FlowsApi['getFlows']>[0] = {}) {
    const api = new FlowsApi(this.session);
    yield* this.fetchAll('GET:/flows/', (pageCursor) =>
      api.getFlows({
        ...options,
        pageCursor,
      })
    );
  }

  async *getFlowFlowActions(
    id: Parameters<FlowsApi['getFlowFlowActions']>[0],
    options: Parameters<FlowsApi['getFlowFlowActions']>[1] = {}
  ) {
    const api = new FlowsApi(this.session);
    yield* this.fetchAll('GET:/flows/{id}/flow-actions/', (pageCursor) =>
      api.getFlowFlowActions(id, {
        ...options,
        pageCursor,
      })
    );
  }

  async *getTemplates(
    options: Parameters<TemplatesApi['getTemplates']>[0] = {}
  ) {
    const api = new TemplatesApi(this.session);
    yield* this.fetchAll('GET:/templates/', (pageCursor) =>
      api.getTemplates({
        ...options,
        pageCursor,
      })
    );
  }

  async *getListProfiles(
    id: Parameters<ListsApi['getListProfiles']>[0],
    options: Parameters<ListsApi['getListProfiles']>[1] = {}
  ) {
    const api = new ListsApi(this.session);
    yield* this.fetchAll('GET:/lists/{id}/profiles/', (pageCursor) =>
      api.getListProfiles(id, {
        pageSize: 100,
        ...options,
        pageCursor,
      })
    );
  }

  async *getSegmentProfiles(
    id: Parameters<SegmentsApi['getSegmentProfiles']>[0],
    options: Parameters<SegmentsApi['getSegmentProfiles']>[1] = {}
  ) {
    const api = new SegmentsApi(this.session);
    yield* this.fetchAll('GET:/segments/{id}/profiles/', (pageCursor) =>
      api.getSegmentProfiles(id, {
        pageSize: 100,
        ...options,
        pageCursor,
      })
    );
  }

  async getAccountId() {
    if (!this.accountId) {
      const {body} = await this.accounts.getAccounts();
      this.accountId = body.data[0].id;
    }
    if (!this.accountId) {
      throw new Error(`Count not find account id.`);
    }
    return this.accountId;
  }

  async getFirstEventDate() {
    return (
      (
        await this.events.getEvents({
          sort: 'timestamp',
        })
      ).body.data[0].attributes.datetime ?? null
    );
  }

  async getDailyEventsCount(options: {cutoff?: Date} = {}) {
    const cutoff = options.cutoff ?? (await this.getFirstEventDate());
    if (!cutoff) {
      throw new Error(`Could not find cutoff date.`);
    }
    const startMoment = moment.utc(cutoff).startOf('day');
    // get intervals
    const intervals: Array<Moment> = [];
    for (
      let d = startMoment.clone();
      d.isBefore(moment.utc().startOf('day'));
      d.add(1, 'year')
    ) {
      intervals.push(d.clone());
    }
    // fetch metrics
    const metrics = await this.getMetrics();
    const counts = await Promise.all(
      metrics.map((m) =>
        Promise.all(
          intervals.map((i) =>
            this.withRetry(() =>
              this.limiters['POST:/metric-aggregates/'](() => {
                return this.metrics.queryMetricAggregates({
                  data: {
                    type: 'metric-aggregate',
                    attributes: {
                      metricId: m.id,
                      measurements: ['count'],
                      interval: 'day',
                      filter: [
                        `greater-or-equal(datetime,${i.toISOString()})`,
                        `less-than(datetime,${i.clone().add(1, 'year').toISOString()})`,
                      ],
                    },
                  },
                });
              })
            )
          )
        )
      )
    );
    const aggregates: Record<string, number> = {};
    _.flatten(_.flatten(counts)).forEach((m) => {
      const counts: number[] = (
        m.body.data.attributes.data[0].measurements as any
      ).count;
      const dates = m.body.data.attributes.dates;
      dates.forEach((d, i) => {
        const key = moment.utc(d).format('YYYY-MM-DD');
        if (!aggregates[key]) {
          aggregates[key] = 0;
        }
        aggregates[key] += counts[i];
      });
    });
    return _.pickBy(aggregates, (v) => v !== 0);
  }

  get campaigns() {
    return new CampaignsApi(this.session);
  }

  get events() {
    return new EventsApi(this.session);
  }

  get templates() {
    return new TemplatesApi(this.session);
  }

  get flows() {
    return new FlowsApi(this.session);
  }

  get lists() {
    return new ListsApi(this.session);
  }

  get segments() {
    return new SegmentsApi(this.session);
  }

  get profiles() {
    return new ProfilesApi(this.session);
  }

  get accounts() {
    return new AccountsApi(this.session);
  }

  get metrics() {
    return new MetricsApi(this.session);
  }
}

export function getKlaviyoOAuthSession(options: {
  clientId: string;
  clientSecret: string;
  refreshToken: string;
}) {
  const oauthApi = new OAuthApi(
    options.clientId,
    options.clientSecret,
    new KlaviyoTokenStorage(options.refreshToken)
  );
  // limit refresh token requests
  const originalRefreshTokens = oauthApi.refreshTokens.bind(oauthApi);
  oauthApi.refreshTokens = throat(
    1,
    function (
      customerIdentifier: string,
      refreshToken?: string
    ): Promise<CreatedTokens> {
      return backOff(
        () => originalRefreshTokens(customerIdentifier, refreshToken),
        {
          startingDelay: 1000,
          timeMultiple: 2,
          maxDelay: 30000,
          numOfAttempts: 10,
          retry(err: KlaviyoTokenError) {
            return (
              err.name === KlaviyoErrorName.refreshError &&
              err.message.includes('rate_limit_exceeded')
            );
          },
        }
      );
    }
  );
  return new OAuthSession(options.refreshToken, oauthApi);
}

export class KlaviyoTokenStorage implements TokenStorage {
  static tokens: Record<string, RetrievedTokens> = {};

  constructor(refreshToken: string) {
    if (!KlaviyoTokenStorage.tokens[refreshToken]) {
      KlaviyoTokenStorage.tokens[refreshToken] = {
        accessToken: '',
        refreshToken,
        expiresAt: new Date(0),
      };
    }
  }

  async retrieve(refreshToken: string): Promise<RetrievedTokens> {
    return KlaviyoTokenStorage.tokens[refreshToken];
  }

  async save(refreshToken: string, token: CreatedTokens) {
    KlaviyoTokenStorage.tokens[refreshToken] = token;
  }
}

const KLAVIYO_ENDPOINTS = {
  'GET:/metric-properties/{id}/': {
    burst: 1,
    steady: 15,
    scopes: ['metrics:read'],
  },
  'GET:/metric-properties/{id}/relationships/metric/': {
    burst: 1,
    steady: 15,
    scopes: ['metrics:read'],
  },
  'GET:/metrics/{id}/metric-properties/': {
    burst: 1,
    steady: 15,
    scopes: ['metrics:read'],
  },
  'GET:/metrics/{id}/relationships/metric-properties/': {
    burst: 1,
    steady: 15,
    scopes: ['metrics:read'],
  },
  'GET:/metric-properties/{id}/metric/': {
    burst: 1,
    steady: 15,
    scopes: ['metrics:read'],
  },
  'GET:/profile-suppression-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['subscriptions:read'],
  },
  'POST:/profile-suppression-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:write', 'subscriptions:write'],
  },
  'GET:/profile-suppression-bulk-create-jobs/{job_id}/': {
    burst: 75,
    steady: 700,
    scopes: ['subscriptions:read'],
  },
  'GET:/profile-suppression-bulk-delete-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['subscriptions:read'],
  },
  'POST:/profile-suppression-bulk-delete-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['subscriptions:write'],
  },
  'GET:/profile-suppression-bulk-delete-jobs/{job_id}/': {
    burst: 75,
    steady: 700,
    scopes: ['subscriptions:read'],
  },
  'POST:/form-values-reports/': {
    burst: 1,
    steady: 2,
    scopes: ['forms:read'],
  },
  'POST:/form-series-reports/': {
    burst: 1,
    steady: 2,
    scopes: ['forms:read'],
  },
  'POST:/segment-values-reports/': {
    burst: 1,
    steady: 2,
    scopes: ['segments:read'],
  },
  'POST:/segment-series-reports/': {
    burst: 1,
    steady: 2,
    scopes: ['segments:read'],
  },
  'GET:/reviews/': {
    burst: 10,
    steady: 150,
    scopes: ['reviews:read'],
  },
  'GET:/reviews/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['reviews:read'],
  },
  'GET:/template-universal-content/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:read'],
  },
  'POST:/template-universal-content/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:write'],
  },
  'GET:/template-universal-content/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:read'],
  },
  'PATCH:/template-universal-content/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:write'],
  },
  'DELETE:/template-universal-content/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:write'],
  },
  'GET:/tracking-settings/': {
    burst: 10,
    steady: 150,
    scopes: ['tracking-settings:read'],
  },
  'GET:/tracking-settings/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['tracking-settings:read'],
  },
  'PATCH:/tracking-settings/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['tracking-settings:write'],
  },
  'GET:/accounts/': {
    burst: 1,
    steady: 15,
    scopes: ['accounts:read'],
  },
  'GET:/accounts/{id}/': {
    burst: 1,
    steady: 15,
    scopes: ['accounts:read'],
  },
  'GET:/campaign-send-jobs/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'PATCH:/campaign-send-jobs/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'GET:/campaign-recipient-estimation-jobs/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'POST:/campaign-send-jobs/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'POST:/campaign-recipient-estimation-jobs/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'GET:/campaigns/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'POST:/campaigns/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'GET:/campaigns/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'PATCH:/campaigns/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'DELETE:/campaigns/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'GET:/campaign-recipient-estimations/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'POST:/campaign-clone/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'GET:/campaign-messages/{id}/campaign/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'GET:/campaign-messages/{id}/template/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read', 'templates:read'],
  },
  'GET:/campaigns/{id}/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['campaigns:read', 'tags:read'],
  },
  'GET:/campaigns/{id}/campaign-messages/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'GET:/campaign-messages/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'PATCH:/campaign-messages/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'POST:/campaign-message-assign-template/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:write'],
  },
  'GET:/campaign-messages/{id}/relationships/campaign/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'GET:/campaign-messages/{id}/relationships/template/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read', 'templates:read'],
  },
  'GET:/campaigns/{id}/relationships/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['campaigns:read', 'tags:read'],
  },
  'GET:/campaigns/{id}/relationships/campaign-messages/': {
    burst: 10,
    steady: 150,
    scopes: ['campaigns:read'],
  },
  'GET:/catalog-items/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-items/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-items/{id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'PATCH:/catalog-items/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'DELETE:/catalog-items/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-item-bulk-create-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-item-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-item-bulk-create-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-item-bulk-update-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-item-bulk-update-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-item-bulk-update-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-item-bulk-delete-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-item-bulk-delete-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-item-bulk-delete-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-categories/{id}/items/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-categories/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-categories/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-categories/{id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'PATCH:/catalog-categories/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'DELETE:/catalog-categories/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-category-bulk-create-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-category-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-category-bulk-create-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-category-bulk-update-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-category-bulk-update-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-category-bulk-update-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-category-bulk-delete-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-category-bulk-delete-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-category-bulk-delete-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-items/{id}/categories/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-variants/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-variants/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-variants/{id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'PATCH:/catalog-variants/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'DELETE:/catalog-variants/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-variant-bulk-create-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-variant-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-variant-bulk-create-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-variant-bulk-update-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-variant-bulk-update-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-variant-bulk-update-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-variant-bulk-delete-jobs/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-variant-bulk-delete-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-variant-bulk-delete-jobs/{job_id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'GET:/catalog-items/{id}/variants/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/back-in-stock-subscriptions/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:write', 'profiles:write'],
  },
  'GET:/catalog-categories/{id}/relationships/items/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-categories/{id}/relationships/items/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'PATCH:/catalog-categories/{id}/relationships/items/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'DELETE:/catalog-categories/{id}/relationships/items/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'GET:/catalog-items/{id}/relationships/categories/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:read'],
  },
  'POST:/catalog-items/{id}/relationships/categories/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'PATCH:/catalog-items/{id}/relationships/categories/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'DELETE:/catalog-items/{id}/relationships/categories/': {
    burst: 75,
    steady: 700,
    scopes: ['catalogs:write'],
  },
  'POST:https://a.klaviyo.com/client/subscriptions/': {
    burst: 100,
    steady: 700,
    scopes: ['subscriptions:write'],
  },
  'POST:https://a.klaviyo.com/client/push-tokens/': {
    burst: 3,
    steady: 150,
    scopes: [],
  },
  'POST:https://a.klaviyo.com/client/push-token-unregister/': {
    burst: 3,
    steady: 60,
    scopes: [],
  },
  'POST:https://a.klaviyo.com/client/events/': {
    burst: 350,
    steady: 3500,
    scopes: ['events:write'],
  },
  'POST:https://a.klaviyo.com/client/profiles/': {
    burst: 350,
    steady: 3500,
    scopes: ['profiles:write'],
  },
  'POST:https://a.klaviyo.com/client/event-bulk-create/': {
    burst: 10,
    steady: 150,
    scopes: ['events:write'],
  },
  'POST:https://a.klaviyo.com/client/back-in-stock-subscriptions/': {
    burst: 350,
    steady: 3500,
    scopes: ['catalogs:write', 'profiles:write'],
  },
  'GET:/coupons/': {
    burst: 75,
    steady: 700,
    scopes: ['coupons:read'],
  },
  'POST:/coupons/': {
    burst: 3,
    steady: 60,
    scopes: ['coupons:write'],
  },
  'GET:/coupons/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['coupons:read'],
  },
  'PATCH:/coupons/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['coupons:write'],
  },
  'DELETE:/coupons/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['coupons:write'],
  },
  'GET:/coupon-codes/': {
    burst: 350,
    steady: 3500,
    scopes: ['coupon-codes:read'],
  },
  'POST:/coupon-codes/': {
    burst: 350,
    steady: 3500,
    scopes: ['coupon-codes:write'],
  },
  'GET:/coupon-codes/{id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['coupon-codes:read'],
  },
  'PATCH:/coupon-codes/{id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['coupon-codes:write'],
  },
  'DELETE:/coupon-codes/{id}/': {
    burst: 350,
    steady: 3500,
    scopes: ['coupon-codes:write'],
  },
  'GET:/coupon-code-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['coupon-codes:read'],
  },
  'POST:/coupon-code-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['coupon-codes:write'],
  },
  'GET:/coupon-code-bulk-create-jobs/{job_id}/': {
    burst: 75,
    steady: 700,
    scopes: ['coupon-codes:read'],
  },
  'GET:/coupon-codes/{id}/coupon/': {
    burst: 75,
    steady: 700,
    scopes: ['coupons:read'],
  },
  'GET:/coupon-codes/{id}/relationships/coupon/': {
    burst: 75,
    steady: 700,
    scopes: ['coupons:read'],
  },
  'GET:/coupons/{id}/coupon-codes/': {
    burst: 75,
    steady: 700,
    scopes: ['coupon-codes:read'],
  },
  'GET:/coupons/{id}/relationships/coupon-codes/': {
    burst: 75,
    steady: 700,
    scopes: ['coupon-codes:read'],
  },
  'POST:/data-privacy-deletion-jobs/': {
    burst: 3,
    steady: 60,
    scopes: ['data-privacy:write'],
  },
  'GET:/events/': {
    burst: 350,
    steady: 3500,
    scopes: ['events:read'],
  },
  'POST:/events/': {
    burst: 350,
    steady: 3500,
    scopes: ['events:write'],
  },
  'GET:/events/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['events:read'],
  },
  'POST:/event-bulk-create-jobs/': {
    burst: 10,
    steady: 150,
    scopes: ['events:write'],
  },
  'GET:/events/{id}/metric/': {
    burst: 350,
    steady: 3500,
    scopes: ['events:read', 'metrics:read'],
  },
  'GET:/events/{id}/profile/': {
    burst: 350,
    steady: 3500,
    scopes: ['events:read', 'profiles:read'],
  },
  'GET:/events/{id}/relationships/metric/': {
    burst: 350,
    steady: 3500,
    scopes: ['events:read', 'metrics:read'],
  },
  'GET:/events/{id}/relationships/profile/': {
    burst: 350,
    steady: 3500,
    scopes: ['events:read', 'profiles:read'],
  },
  'GET:/flows/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flows/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'PATCH:/flows/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:write'],
  },
  'DELETE:/flows/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:write'],
  },
  'GET:/flow-actions/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flow-messages/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flows/{id}/flow-actions/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flows/{id}/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read', 'tags:read'],
  },
  'GET:/flow-actions/{id}/flow/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flow-actions/{id}/flow-messages/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flow-messages/{id}/flow-action/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flows/{id}/relationships/flow-actions/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flows/{id}/relationships/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read', 'tags:read'],
  },
  'GET:/flow-actions/{id}/relationships/flow/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flow-actions/{id}/relationships/flow-messages/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flow-messages/{id}/relationships/flow-action/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read'],
  },
  'GET:/flow-messages/{id}/relationships/template/': {
    burst: 3,
    steady: 60,
    scopes: ['templates:read'],
  },
  'GET:/flow-messages/{id}/template/': {
    burst: 3,
    steady: 60,
    scopes: ['templates:read'],
  },
  'GET:/forms/{id}/form-versions/': {
    burst: 3,
    steady: 60,
    scopes: ['forms:read'],
  },
  'GET:/forms/{id}/relationships/form-versions/': {
    burst: 3,
    steady: 60,
    scopes: ['forms:read'],
  },
  'GET:/form-versions/{id}/relationships/form/': {
    burst: 3,
    steady: 60,
    scopes: ['forms:read'],
  },
  'GET:/form-versions/{id}/form/': {
    burst: 3,
    steady: 60,
    scopes: ['forms:read'],
  },
  'GET:/forms/': {
    burst: 3,
    steady: 60,
    scopes: ['forms:read'],
  },
  'GET:/forms/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['forms:read'],
  },
  'GET:/form-versions/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['forms:read'],
  },
  'GET:/images/': {
    burst: 10,
    steady: 150,
    scopes: ['images:read'],
  },
  'POST:/images/': {
    burst: 3,
    steady: 100,
    scopes: ['images:write'],
  },
  'GET:/images/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['images:read'],
  },
  'PATCH:/images/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['images:write'],
  },
  'POST:/image-upload/': {
    burst: 3,
    steady: 100,
    scopes: ['images:write'],
  },
  'GET:/lists/': {
    burst: 75,
    steady: 700,
    scopes: ['lists:read'],
  },
  'POST:/lists/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:write'],
  },
  'GET:/lists/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['lists:read'],
  },
  'PATCH:/lists/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:write'],
  },
  'DELETE:/lists/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:write'],
  },
  'GET:/lists/{id}/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['lists:read', 'tags:read'],
  },
  'POST:/lists/{id}/relationships/profiles/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:write', 'profiles:write'],
  },
  'DELETE:/lists/{id}/relationships/profiles/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:write', 'profiles:write'],
  },
  'GET:/lists/{id}/profiles/': {
    burst: 75,
    steady: 700,
    scopes: ['lists:read', 'profiles:read'],
  },
  'GET:/lists/{id}/relationships/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['lists:read', 'tags:read'],
  },
  'GET:/lists/{id}/relationships/profiles/': {
    burst: 75,
    steady: 700,
    scopes: ['lists:read', 'profiles:read'],
  },
  'GET:/metrics/': {
    burst: 10,
    steady: 150,
    scopes: ['metrics:read'],
  },
  'GET:/metrics/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['metrics:read'],
  },
  'POST:/metric-aggregates/': {
    burst: 3,
    steady: 60,
    scopes: ['metrics:read'],
  },
  'GET:/profile-bulk-import-jobs/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:read', 'profiles:read'],
  },
  'POST:/profile-bulk-import-jobs/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:write', 'profiles:write'],
  },
  'GET:/profile-bulk-import-jobs/{job_id}/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:read', 'profiles:read'],
  },
  'GET:/profile-bulk-import-jobs/{id}/lists/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:read'],
  },
  'GET:/profile-bulk-import-jobs/{id}/relationships/lists/': {
    burst: 10,
    steady: 150,
    scopes: ['lists:read'],
  },
  'GET:/profile-bulk-import-jobs/{id}/profiles/': {
    burst: 10,
    steady: 150,
    scopes: ['profiles:read'],
  },
  'GET:/profile-bulk-import-jobs/{id}/relationships/profiles/': {
    burst: 10,
    steady: 150,
    scopes: ['profiles:read'],
  },
  'GET:/profile-bulk-import-jobs/{id}/import-errors/': {
    burst: 10,
    steady: 150,
    scopes: ['profiles:read'],
  },
  'GET:/profiles/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:read'],
  },
  'POST:/profiles/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:write'],
  },
  'GET:/profiles/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:read'],
  },
  'PATCH:/profiles/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:write'],
  },
  'POST:/profile-import/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:write'],
  },
  'POST:/profile-merge/': {
    burst: 10,
    steady: 150,
    scopes: ['profiles:write'],
  },
  'POST:/profile-subscription-bulk-create-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['lists:write', 'profiles:write', 'subscriptions:write'],
  },
  'POST:/profile-subscription-bulk-delete-jobs/': {
    burst: 75,
    steady: 700,
    scopes: ['lists:write', 'profiles:write', 'subscriptions:write'],
  },
  'POST:/push-tokens/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:write', 'push-tokens:write'],
  },
  'GET:/profiles/{id}/lists/': {
    burst: 3,
    steady: 60,
    scopes: ['lists:read', 'profiles:read'],
  },
  'GET:/profiles/{id}/segments/': {
    burst: 3,
    steady: 60,
    scopes: ['profiles:read', 'segments:read'],
  },
  'GET:/profiles/{id}/relationships/lists/': {
    burst: 3,
    steady: 60,
    scopes: ['lists:read', 'profiles:read'],
  },
  'GET:/profiles/{id}/relationships/segments/': {
    burst: 3,
    steady: 60,
    scopes: ['profiles:read', 'segments:read'],
  },
  'POST:/campaign-values-reports/': {
    burst: 1,
    steady: 2,
    scopes: ['campaigns:read'],
  },
  'POST:/flow-values-reports/': {
    burst: 1,
    steady: 2,
    scopes: ['flows:read'],
  },
  'POST:/flow-series-reports/': {
    burst: 1,
    steady: 2,
    scopes: ['flows:read'],
  },
  'GET:/segments/': {
    burst: 75,
    steady: 700,
    scopes: ['segments:read'],
  },
  'POST:/segments/': {
    burst: 1,
    steady: 15,
    scopes: ['segments:write'],
  },
  'GET:/segments/{id}/': {
    burst: 75,
    steady: 700,
    scopes: ['segments:read'],
  },
  'PATCH:/segments/{id}/': {
    burst: 1,
    steady: 15,
    scopes: ['segments:write'],
  },
  'DELETE:/segments/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['segments:write'],
  },
  'GET:/segments/{id}/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['segments:read', 'tags:read'],
  },
  'GET:/segments/{id}/profiles/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:read', 'segments:read'],
  },
  'GET:/segments/{id}/relationships/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['segments:read', 'tags:read'],
  },
  'GET:/segments/{id}/relationships/profiles/': {
    burst: 75,
    steady: 700,
    scopes: ['profiles:read', 'segments:read'],
  },
  'GET:/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'POST:/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read', 'tags:write'],
  },
  'GET:/tags/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'PATCH:/tags/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read', 'tags:write'],
  },
  'DELETE:/tags/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read', 'tags:write'],
  },
  'GET:/tags/{id}/tag-group/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'GET:/tag-groups/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'POST:/tag-groups/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read', 'tags:write'],
  },
  'GET:/tag-groups/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'PATCH:/tag-groups/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read', 'tags:write'],
  },
  'DELETE:/tag-groups/{id}/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read', 'tags:write'],
  },
  'GET:/tag-groups/{id}/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'GET:/tags/{id}/relationships/flows/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:read', 'tags:read'],
  },
  'POST:/tags/{id}/relationships/flows/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:writetags:write'],
  },
  'DELETE:/tags/{id}/relationships/flows/': {
    burst: 3,
    steady: 60,
    scopes: ['flows:write', 'tags:write'],
  },
  'GET:/tags/{id}/relationships/campaigns/': {
    burst: 3,
    steady: 60,
    scopes: ['campaigns:read', 'tags:read'],
  },
  'POST:/tags/{id}/relationships/campaigns/': {
    burst: 3,
    steady: 60,
    scopes: ['campaigns:write', 'tags:write'],
  },
  'DELETE:/tags/{id}/relationships/campaigns/': {
    burst: 3,
    steady: 60,
    scopes: ['campaigns:write', 'tags:write'],
  },
  'GET:/tags/{id}/relationships/lists/': {
    burst: 3,
    steady: 60,
    scopes: ['lists:read', 'tags:read'],
  },
  'POST:/tags/{id}/relationships/lists/': {
    burst: 3,
    steady: 60,
    scopes: ['lists:write', 'tags:write'],
  },
  'DELETE:/tags/{id}/relationships/lists/': {
    burst: 3,
    steady: 60,
    scopes: ['lists:write', 'tags:write'],
  },
  'GET:/tags/{id}/relationships/segments/': {
    burst: 3,
    steady: 60,
    scopes: ['segments:read', 'tags:read'],
  },
  'POST:/tags/{id}/relationships/segments/': {
    burst: 3,
    steady: 60,
    scopes: ['segments:write', 'tags:write'],
  },
  'DELETE:/tags/{id}/relationships/segments/': {
    burst: 3,
    steady: 60,
    scopes: ['segments:write', 'tags:write'],
  },
  'GET:/tags/{id}/relationships/tag-group/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'GET:/tag-groups/{id}/relationships/tags/': {
    burst: 3,
    steady: 60,
    scopes: ['tags:read'],
  },
  'GET:/templates/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:read'],
  },
  'POST:/templates/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:write'],
  },
  'GET:/templates/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:read'],
  },
  'PATCH:/templates/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:write'],
  },
  'DELETE:/templates/{id}/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:write'],
  },
  'POST:/template-render/': {
    burst: 3,
    steady: 60,
    scopes: ['templates:read'],
  },
  'POST:/template-clone/': {
    burst: 10,
    steady: 150,
    scopes: ['templates:write'],
  },
  'GET:/webhooks/': {
    burst: 1,
    steady: 15,
    scopes: ['webhooks:read'],
  },
  'POST:/webhooks/': {
    burst: 1,
    steady: 15,
    scopes: ['webhooks:write'],
  },
  'GET:/webhooks/{id}/': {
    burst: 1,
    steady: 15,
    scopes: ['webhooks:read'],
  },
  'PATCH:/webhooks/{id}/': {
    burst: 1,
    steady: 15,
    scopes: ['webhooks:write'],
  },
  'DELETE:/webhooks/{id}/': {
    burst: 1,
    steady: 15,
    scopes: ['webhooks:write'],
  },
  'GET:/webhook-topics/': {
    burst: 1,
    steady: 15,
    scopes: ['webhooks:read'],
  },
  'GET:/webhook-topics/{id}/': {
    burst: 1,
    steady: 15,
    scopes: ['webhooks:read'],
  },
};
