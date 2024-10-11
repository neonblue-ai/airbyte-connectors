import {SyncMode} from 'faros-airbyte-cdk';
import _ from 'lodash';
import moment from 'moment';
import {Dictionary} from 'ts-essentials';
import {z} from 'zod';

import {FlowRecord} from '../schemas/FlowRecord';
import {fromApiRecordAttributes, fromZodType, KlaviyoStream} from './klaviyo';

// record
const Record = FlowRecord.extend({
  account_id: z.string(),
});
type Record = z.infer<typeof Record>;

// state
type FlowStreamState = {
  cutoff: number;
};

export class Flows extends KlaviyoStream {
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
    streamState?: FlowStreamState
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
    for await (const items of this.client.getFlows({
      filter: [
        `greater-or-equal(${this.cursorField},${this.client.toDatetimeFilterString(lastCutoff)})`,
      ].join(','),
      sort: this.cursorField,
    })) {
      this.controller.signal.throwIfAborted();
      const childItems = items.map((item, i) => {
        return Promise.all([
          this.client.withLimiter('GET:/flows/{id}/flow-actions/', () => {
            this.controller.signal.throwIfAborted();
            return this.client.flows.getFlowFlowActions(item.id);
          }),
          this.client.withLimiter('GET:/flows/{id}/tags/', () => {
            this.controller.signal.throwIfAborted();
            return this.client.flows.getFlowTags(item.id);
          }),
        ]);
      });
      for (let j = 0; j < items.length; j++) {
        this.controller.signal.throwIfAborted();
        const item = items[j];
        const [actions, tags] = await childItems[j];
        const messages = await Promise.all(
          actions.body.data.map((a) =>
            this.client.withLimiter(
              'GET:/flow-actions/{id}/flow-messages/',
              () => this.client.flows.getFlowActionMessages(a.id)
            )
          )
        );
        const messageIdToTemplateIdMap = _.fromPairs(
          await Promise.all(
            _.flatMap(messages, (m) => m.body.data.map((mm) => mm.id)).map(
              (messageId) =>
                this.client.withLimiter(
                  'GET:/flow-messages/{id}/relationships/template/',
                  async () => {
                    const templateResult =
                      await this.client.flows.getFlowMessageRelationshipsTemplate(
                        messageId
                      );
                    return [messageId, templateResult.body?.data?.id] as [
                      string,
                      string,
                    ];
                  }
                )
            )
          )
        );
        yield {
          id: item.id,
          ...fromApiRecordAttributes(item.attributes),
          flow_action_ids: item?.relationships?.flowActions?.data?.map(
            (i) => i.id
          ),
          tag_ids: item?.relationships?.tags?.data?.map((i) => i.id),
          account_id: accountId,
          flow_actions: actions.body.data.map((action, i) => ({
            id: action.id,
            ...fromApiRecordAttributes(action.attributes),
            flow_message_ids: messages[i].body?.data?.map((m) => m.id),
            flow_messages: messages[i].body?.data.map((m) => ({
              id: m.id,
              ...fromApiRecordAttributes(m.attributes),
              template_id: messageIdToTemplateIdMap[m.id],
            })),
          })),
          tags: tags.body.data.map((t) => t.attributes.name),
        } as Record;
      }
    }
  }

  getUpdatedState(
    currentStreamState: FlowStreamState,
    latestRecord: Record,
    streamSlice?: Dictionary<string, any>
  ): FlowStreamState {
    return {
      cutoff: Math.max(
        currentStreamState?.cutoff ?? 0,
        moment.utc(latestRecord[this.cursorField]).toDate().getTime()
      ),
    };
  }
}
