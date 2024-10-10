import {SyncMode} from 'faros-airbyte-cdk';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';
import {z} from 'zod';

import {CampaignRecord} from '../schemas/CampaignRecord';
import {fromApiRecordAttributes, fromZodType, KlaviyoStream} from './klaviyo';

// record
const Record = CampaignRecord.extend({
  account_id: z.string(),
});
type Record = z.infer<typeof Record>;

// state
type CampaignStreamState = {
  cutoff: number;
};

export class Campaigns extends KlaviyoStream {
  getJsonSchema(): Dictionary<any, string> {
    return fromZodType(Record);
  }

  get primaryKey(): keyof Record {
    return 'id';
  }

  get cursorField(): Extract<keyof Record, 'created_at' | 'updated_at'> {
    return 'updated_at';
  }

  async *readRecords(
    syncMode: SyncMode,
    cursorField?: string[],
    streamSlice?: Dictionary<string, any>,
    streamState?: CampaignStreamState
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
    for (const channel of ['email', 'sms']) {
      for await (const items of this.client.getCampaigns(
        [
          `equals(messages.channel,'${channel}')`,
          `equals(status,'Sent')`,
          `greater-or-equal(${this.cursorField},${this.client.toDatetimeFilterString(lastCutoff)})`,
        ].join(','),
        {
          sort: this.cursorField,
        }
      )) {
        const childItems = items.map((item, i) => {
          return Promise.all([
            this.client.withLimiter(
              'GET:/campaigns/{id}/campaign-messages/',
              () => {
                this.controller.signal.throwIfAborted();
                return this.client.campaigns.getCampaignCampaignMessages(
                  item.id
                );
              }
            ),
            this.client.withLimiter('GET:/campaigns/{id}/tags/', () => {
              this.controller.signal.throwIfAborted();
              return this.client.campaigns.getCampaignTags(item.id);
            }),
          ]);
        });
        for (let j = 0; j < items.length; j++) {
          const item = items[j];
          const [messages, tags] = await childItems[j];
          yield {
            id: item.id,
            channel,
            ...fromApiRecordAttributes(item.attributes),
            campaign_message_ids:
              item.relationships?.campaignMessages?.data?.map((i) => i.id),
            tag_ids: item?.relationships?.tags?.data?.map((i) => i.id),
            account_id: accountId,
            campaign_messages: messages.body.data.map((m) => ({
              id: m.id,
              ...m.attributes,
              template_id: m.relationships?.template?.data?.id,
            })),
            tags: tags.body.data.map((t) => t.attributes.name),
          } as Record;
        }
      }
    }
  }

  getUpdatedState(
    currentStreamState: CampaignStreamState,
    latestRecord: Record,
    streamSlice?: Dictionary<string, any>
  ): CampaignStreamState {
    return {
      cutoff: Math.max(
        currentStreamState?.cutoff ?? 0,
        moment.utc(latestRecord[this.cursorField]).toDate().getTime()
      ),
    };
  }
}
