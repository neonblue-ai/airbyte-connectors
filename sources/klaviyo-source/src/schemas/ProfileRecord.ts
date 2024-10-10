import {z} from 'zod';

export const ProfileRecord = z.object({
  id: z.string().optional(),
  email: z.string().optional(),
  phone_number: z.string().optional(),
  external_id: z.string().optional(),
  first_name: z.string().optional(),
  last_name: z.string().optional(),
  organization: z.string().optional(),
  locale: z.string().optional(),
  title: z.string().optional(),
  image: z.string().optional(),
  created: z.string().datetime({offset: true}).optional(),
  updated: z.string().datetime({offset: true}).optional(),
  last_event_date: z.string().datetime({offset: true}).optional(),
  location: z.record(z.any()).optional(),
  properties: z.record(z.any()).optional(),
  subscriptions: z.record(z.any()).optional(),
  predictive_analytics: z.record(z.any()).optional(),
});
