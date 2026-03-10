import { z } from "zod";

export const ForceSyncInputSchema = z.object({
  modId: z.string().optional(),
});

export const ForceSyncOutputSchema = z.object({
  success: z.boolean(),
  message: z.string(),
});
