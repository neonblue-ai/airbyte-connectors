import {SyncMode} from 'faros-airbyte-cdk';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';
import {z} from 'zod';

import {ProfileRecord} from '../schemas/ProfileRecord';
import {
  fromApiRecordAttributes,
  fromZodType,
  KlaviyoStream,
  momentRanges,
} from './klaviyo';

// record
const Record = ProfileRecord.extend({
  account_id: z.string(),
});
type Record = z.infer<typeof Record>;

// state
type ProfileStreamState = {
  cutoff: number;
};

// stream
export class Profiles extends KlaviyoStream {
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
    streamState?: ProfileStreamState
  ) {
    const accountId = await this.client.withRetry(() =>
      this.client.getAccountId()
    );
    this.logger.info(
      `Read records with: ${JSON.stringify({syncMode, cursorField, streamSlice, streamState})}`
    );
    let lastCutoff: number = streamState?.cutoff ?? 0;
    if (!lastCutoff || syncMode === SyncMode.FULL_REFRESH) {
      const firstItem = await this.client.withRetry(() =>
        this.client.profiles.getProfiles({
          sort: this.cursorField,
          pageSize: 1,
        })
      );
      lastCutoff = _.first(firstItem.body.data)?.attributes.created.getTime();
    }
    this.logger.info(`Last cutoff: ${lastCutoff}`);
    const ranges = momentRanges({
      from: moment.utc(lastCutoff),
      step: moment.duration(1, 'hour'),
      stepOverlap: moment.duration(5, 'second'),
      // klaviyo's api doesn't get the ranges precisely...
      startOverlap: moment.duration(1, 'minute'),
    });
    yield* this.parallelSequentialRead(
      {
        parallel: 10,
        dedupe: true,
      },
      ranges,
      async function* ({from, to}) {
        this.controller.signal.throwIfAborted();
        this.logger.info(
          `Fetching ${this.cursorField} from ${from.toISOString()} to ${to.toISOString()}`
        );
        for await (const items of this.client.getProfiles({
          sort: this.cursorField,
          filter: [
            `greater-than(${this.cursorField},${this.client.toDatetimeFilterString(from)})`,
            `less-than(${this.cursorField},${this.client.toDatetimeFilterString(to)})`,
          ].join(','),
        })) {
          this.controller.signal.throwIfAborted();
          for (const item of items) {
            this.controller.signal.throwIfAborted();
            yield {
              id: item.id,
              ...fromApiRecordAttributes(item.attributes),
              account_id: accountId,
            } as Record;
          }
        }
      }
    );
  }

  get stateCheckpointInterval(): number | undefined {
    return 100000;
  }

  getUpdatedState(
    currentStreamState: ProfileStreamState,
    latestRecord: Record,
    streamSlice?: Dictionary<string, any>
  ): ProfileStreamState {
    return {
      cutoff: Math.max(
        currentStreamState?.cutoff ?? 0,
        moment.utc(latestRecord[this.cursorField]).toDate().getTime()
      ),
    };
  }
}
