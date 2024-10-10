import {z} from 'zod';

export const CampaignRecord = z.object({
  id: z.string().optional(),
  name: z.string().optional(),
  status: z.string().optional(),
  archived: z.boolean().optional(),
  audiences: z.record(z.any()).optional(),
  send_options: z.record(z.any()).optional(),
  tracking_options: z.record(z.any()).optional(),
  send_strategy: z.record(z.any()).optional(),
  created_at: z.string().datetime({offset: true}).optional(),
  scheduled_at: z.string().datetime({offset: true}).optional(),
  updated_at: z.string().datetime({offset: true}).optional(),
  send_time: z.string().datetime({offset: true}).optional(),
  campaign_message_ids: z.array(z.string()).optional(),
  tag_ids: z.array(z.string()).optional(),
});
