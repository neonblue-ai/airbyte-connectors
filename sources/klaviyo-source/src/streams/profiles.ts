import {SyncMode} from 'faros-airbyte-cdk';
import {ProfileResponseObjectResourceExtendedAttributes} from 'klaviyo-api';
import _ from 'lodash';
import moment, {Moment} from 'moment';
import {Dictionary} from 'ts-essentials';

import {KlaviyoStream} from './klaviyo';

type ProfileRecord = {
  id: string;
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
    this.logger.info(
      `Read records with: ${JSON.stringify({syncMode, cursorField, streamSlice, streamState})}`
    );
    let lastCutoff: number = streamState?.cutoff ?? 0;
    if (!lastCutoff || syncMode === SyncMode.FULL_REFRESH) {
      const firstProfile = await this.client.withRetry(() =>
        this.client.profiles.getProfiles({
          sort: this.realCursorField,
          pageSize: 1,
        })
      );
      lastCutoff = _.first(
        firstProfile.body.data
      )?.attributes.created.getTime();
    }
    if (lastCutoff > Date.now()) {
      this.logger.info(
        `Last cutoff ${lastCutoff} is greater than current time.`
      );
      return;
    }
    this.logger.info(`Last cutoff: ${lastCutoff}`);
    const interval = moment.duration(1, 'hour');
    const ranges: Array<{from: Moment; to: Moment}> = [];
    for (
      let d = moment.utc(lastCutoff);
      d.isBefore(moment.utc());
      d.add(interval)
    ) {
      ranges.push({from: d.clone(), to: d.clone().add(interval)});
    }
    yield* this.parallelSequentialRead(
      {
        parallel: 10,
      },
      ranges,
      async function* ({from, to}) {
        this.controller.signal.throwIfAborted();
        this.logger.info(
          `Fetching ${this.realCursorField} profiles from ${from.toISOString()} to ${to.toISOString()}`
        );
        for await (const profiles of this.client.getProfiles({
          sort: this.realCursorField,
          filter: `and(greater-than(${this.realCursorField},${from
            .toISOString()
            .replace('.000', '')}),less-than(${this.realCursorField},${to
            .toISOString()
            .replace('.000', '')}))`,
        })) {
          this.controller.signal.throwIfAborted();
          for (const profile of profiles) {
            this.controller.signal.throwIfAborted();
            yield {
              id: profile.id,
              ..._.mapKeys(profile.attributes, (v, k) => _.snakeCase(k)),
            };
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
    return {
      cutoff: Math.max(
        currentStreamState.cutoff ?? 0,
        new Date(latestRecord[this.realCursorField]).getTime()
      ),
    };
  }
}
