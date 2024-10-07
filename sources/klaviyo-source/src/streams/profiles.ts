import {SyncMode} from 'faros-airbyte-cdk';
import {ProfileResponseObjectResourceExtendedAttributes} from 'klaviyo-api';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';

import {KlaviyoStream, momentRanges} from './klaviyo';

type ProfileRecord = {
  id: string;
  account_id: string;
} & ProfileResponseObjectResourceExtendedAttributes;

type ProfileStreamState = {
  cutoff: number;
};

export class Profiles extends KlaviyoStream {
  getJsonSchema(): Dictionary<any, string> {
    return require('../../resources/schemas/profiles.json');
  }

  get primaryKey(): keyof ProfileRecord {
    return 'id';
  }

  get cursorField(): Extract<keyof ProfileRecord, 'created' | 'updated'>[] {
    return ['updated', 'created'];
  }

  get realCursorField() {
    return this.config.initialize ? this.cursorField[1] : this.cursorField[0];
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
          sort: this.realCursorField,
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
      },
      ranges,
      async function* ({from, to}) {
        this.controller.signal.throwIfAborted();
        this.logger.info(
          `Fetching ${this.realCursorField} from ${from.toISOString()} to ${to.toISOString()}`
        );
        for await (const items of this.client.getProfiles({
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
    currentStreamState: ProfileStreamState,
    latestRecord: ProfileRecord,
    streamSlice?: Dictionary<string, any>
  ): ProfileStreamState {
    this.logger.info(
      `getUpdateState with: ${JSON.stringify({currentStreamState, latestRecord, streamSlice})}`
    );
    return {
      cutoff: Math.max(
        currentStreamState?.cutoff ?? 0,
        moment.utc(latestRecord[this.realCursorField]).toDate().getTime()
      ),
    };
  }
}
