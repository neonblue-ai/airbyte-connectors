import {z} from 'zod';

export const TemplateRecord = z.object({
  id: z.string().optional(),
  name: z.string().optional(),
  editor_type: z.string().optional(),
  html: z.string().optional(),
  text: z.string().optional(),
  created: z.string().datetime({offset: true}).optional(),
  updated: z.string().datetime({offset: true}).optional(),
});
