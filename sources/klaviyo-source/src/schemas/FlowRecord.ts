import {z} from 'zod';

export const FlowRecord = z.object({
  id: z.string().optional(),
  name: z.string().optional(),
  status: z.string().optional(),
  archived: z.boolean().optional(),
  created: z.string().datetime({offset: true}).optional(),
  updated: z.string().datetime({offset: true}).optional(),
  trigger_type: z.string().optional(),
  flow_action_ids: z.array(z.string()).optional(),
  tag_ids: z.array(z.string()).optional(),
});
