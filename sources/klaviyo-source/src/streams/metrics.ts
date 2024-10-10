import {SyncMode} from 'faros-airbyte-cdk';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';
import {z} from 'zod';

import {MetricRecord} from '../schemas/MetricRecord';
import {fromApiRecordAttributes, fromZodType, KlaviyoStream} from './klaviyo';

// record
const Record = MetricRecord.extend({
  account_id: z.string(),
});
type Record = z.infer<typeof Record>;

// state
type MetricStreamState = {
  cutoff: number;
};

export class Metrics extends KlaviyoStream {
  getJsonSchema(): Dictionary<any, string> {
    return fromZodType(Record);
  }

  get primaryKey(): keyof Record {
    return 'id';
  }

  get cursorField(): Extract<keyof Record, 'created' | 'updated'> {
    return this.config.initialize ? 'created' : 'updated';
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
      yield {
        id: item.id,
        ...fromApiRecordAttributes(item.attributes),
        account_id: accountId,
      } as Record;
    }
  }

  getUpdatedState(
    currentStreamState: MetricStreamState,
    latestRecord: Record,
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
