import {SyncMode} from 'faros-airbyte-cdk';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';
import {z} from 'zod';

import {TemplateRecord} from '../schemas/TemplateRecord';
import {fromApiRecordAttributes, fromZodType, KlaviyoStream} from './klaviyo';

// record
const Record = TemplateRecord.extend({
  account_id: z.string(),
});
type Record = z.infer<typeof Record>;

// state
type TemplateStreamState = {
  cutoff: number;
};

export class Templates extends KlaviyoStream {
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
    streamState?: TemplateStreamState
  ) {
    const accountId = await this.client.withRetry(() =>
      this.client.getAccountId()
    );
    this.logger.info(
      `Read records with: ${JSON.stringify({syncMode, cursorField, streamSlice, streamState})}`
    );
    let lastCutoff: number =
      streamState?.cutoff ?? new Date('2000-01-01').getTime();
    if (!lastCutoff || syncMode === SyncMode.FULL_REFRESH) {
      lastCutoff = new Date('2000-01-01').getTime();
    }
    lastCutoff = moment
      .utc(lastCutoff)
      .subtract(moment.duration(1, 'hour'))
      .toDate()
      .getTime();
    this.logger.info(`Last cutoff: ${lastCutoff}`);
    for await (const items of this.client.getTemplates({
      sort: this.cursorField,
      filter: [
        `greater-or-equal(${this.cursorField},${this.client.toDatetimeFilterString(lastCutoff)})`,
      ].join(','),
    })) {
      for (const item of items) {
        yield {
          id: item.id,
          ...fromApiRecordAttributes(item.attributes),
          account_id: accountId,
        } as Record;
      }
    }
  }

  getUpdatedState(
    currentStreamState: TemplateStreamState,
    latestRecord: Record,
    streamSlice?: Dictionary<string, any>
  ): TemplateStreamState {
    return {
      cutoff: Math.max(
        currentStreamState?.cutoff ?? 0,
        moment.utc(latestRecord[this.cursorField]).toDate().getTime()
      ),
    };
  }
}
