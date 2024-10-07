import {SyncMode} from 'faros-airbyte-cdk';
import {EventResponseObjectResourceAttributes} from 'klaviyo-api';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';

import {KlaviyoStream, momentRanges} from './klaviyo';

type EventRecord = {
  id: string;
  profile_id: string;
  metric_id: string;
  account_id: string;
} & EventResponseObjectResourceAttributes;

type EventStreamState = {
  cutoff: number;
};

export class Events extends KlaviyoStream {
  getJsonSchema(): Dictionary<any, string> {
    return require('../../resources/schemas/events.json');
  }

  get primaryKey(): keyof EventRecord {
    return 'id';
  }

  get cursorField(): Extract<keyof EventRecord, 'datetime'>[] {
    return ['datetime'];
  }

  get realCursorField() {
    return this.cursorField[0];
  }

  async *readRecords(
    syncMode: SyncMode,
    cursorField?: string[],
    streamSlice?: Dictionary<string, any>,
    streamState?: EventStreamState
  ) {
    const accountId = await this.client.getAccountId();
    this.logger.info(
      `Read records with: ${JSON.stringify({syncMode, cursorField, streamSlice, streamState})}`
    );
    let lastCutoff: number = streamState?.cutoff ?? 0;
    if (!lastCutoff || syncMode === SyncMode.FULL_REFRESH) {
      const firstItem = await this.client.withRetry(() =>
        this.client.events.getEvents({
          sort: this.realCursorField,
        })
      );
      lastCutoff = _.first(firstItem.body.data)?.attributes.datetime.getTime();
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
        parallel: 20,
      },
      ranges,
      async function* ({from, to}) {
        this.controller.signal.throwIfAborted();
        this.logger.info(
          `Fetching ${this.realCursorField} from ${from.toISOString()} to ${to.toISOString()}`
        );
        for await (const items of this.client.getEvents({
          sort: this.realCursorField,
          filter: `and(greater-than(${this.realCursorField},${from
            .toISOString()
            .replace('.000', '')}),less-than(${this.realCursorField},${to
            .toISOString()
            .replace('.000', '')}))`,
        })) {
          this.controller.signal.throwIfAborted();
          for (const item of items) {
            this.controller.signal.throwIfAborted();
            if (this.shouldEmit(item.id)) {
              yield {
                id: item.id,
                ..._.mapKeys(item.attributes, (v, k) => _.snakeCase(k)),
                metric_id: item.relationships?.metric?.data?.id,
                profile_id: item.relationships?.profile?.data?.id,
                account_id: accountId,
              };
            }
          }
        }
      }
    );
  }

  get stateCheckpointInterval(): number | undefined {
    return 100000;
  }

  getUpdatedState(
    currentStreamState: EventStreamState,
    latestRecord: EventRecord,
    streamSlice?: Dictionary<string, any>
  ): EventStreamState {
    return {
      cutoff: Math.max(
        currentStreamState?.cutoff ?? 0,
        moment.utc(latestRecord[this.realCursorField]).toDate().getTime()
      ),
    };
  }
}
