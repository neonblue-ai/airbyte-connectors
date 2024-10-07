import {SyncMode} from 'faros-airbyte-cdk';
import {MetricResponseObjectResourceAttributes} from 'klaviyo-api';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';

import {KlaviyoStream} from './klaviyo';

type MetricRecord = {
  id: string;
} & MetricResponseObjectResourceAttributes;

type MetricStreamState = {
  cutoff: number;
};

export class Metrics extends KlaviyoStream {
  getJsonSchema(): Dictionary<any, string> {
    return require('../../resources/schemas/metrics.json');
  }

  get primaryKey(): keyof MetricRecord {
    return 'id';
  }

  get cursorField() {
    return undefined;
  }

  async *readRecords(
    syncMode: SyncMode,
    cursorField?: string[],
    streamSlice?: Dictionary<string, any>,
    streamState?: MetricStreamState
  ) {
    const accountId = await this.client.getAccountId();
    this.logger.info(
      `Read records with: ${JSON.stringify({syncMode, cursorField, streamSlice, streamState})}`
    );
    const items = await this.client.getMetrics();
    for (const item of items) {
      this.controller.signal.throwIfAborted();
      if (this.shouldEmit(item.id)) {
        yield {
          id: item.id,
          ..._.mapKeys(item.attributes, (v, k) => _.snakeCase(k)),
          account_id: accountId,
        };
      }
    }
  }

  get stateCheckpointInterval(): number | undefined {
    return 100000;
  }

  getUpdatedState(
    currentStreamState: MetricStreamState,
    latestRecord: MetricRecord,
    streamSlice?: Dictionary<string, any>
  ): MetricStreamState {
    return {
      cutoff: Math.max(
        currentStreamState?.cutoff ?? 0,
        moment.utc(latestRecord.updated).toDate().getTime()
      ),
    };
  }
}
