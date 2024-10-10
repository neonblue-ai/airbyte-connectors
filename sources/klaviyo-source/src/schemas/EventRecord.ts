import {z} from 'zod';

export const EventRecord = z.object({
  id: z.string().optional(),
  timestamp: z.number().optional(),
  event_properties: z.record(z.any()).optional(),
  datetime: z.string().datetime({offset: true}).optional(),
  uuid: z.string().optional(),
  profile_id: z.string().optional(),
  metric_id: z.string().optional(),
  attribution_ids: z.array(z.string()).optional(),
});
