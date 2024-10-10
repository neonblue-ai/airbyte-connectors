import {z} from 'zod';

export const MetricRecord = z.object({
  id: z.string().optional(),
  name: z.string().optional(),
  created: z.string().optional(),
  updated: z.string().optional(),
  integration: z.record(z.any()).optional(),
});
