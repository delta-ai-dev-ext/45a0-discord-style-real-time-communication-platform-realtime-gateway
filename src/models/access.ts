/**
 * Contract for `GET /internal/v1/communities/{community_id}/members/{user_id}/access`
 * on `community_service` — the single source of truth for community authorization.
 */

import { z } from "zod";

export const memberAccessSchema = z.object({
  community_id: z.string().min(1),
  user_id: z.string().min(1),
  is_member: z.boolean(),
  banned: z.boolean(),
  visible_channel_ids: z.array(z.string().min(1)).default([]),
  timed_out_until: z.string().min(1).nullable().optional(),
});

export type MemberAccess = z.infer<typeof memberAccessSchema>;

export interface MemberAccessQuery {
  readonly communityId: string;
  readonly userId: string;
  /** W3C trace context propagated from the triggering Kafka envelope (or synthesized). */
  readonly traceparent: string;
}

export const ACCESS_DENIED_REASONS = ["not_member", "banned"] as const;

export type AccessDeniedReason = (typeof ACCESS_DENIED_REASONS)[number];

export type AccessDecision =
  | {
      readonly ok: true;
      readonly visibleChannelIds: readonly string[];
      readonly timedOutUntil: string | null;
    }
  | { readonly ok: false; readonly reason: AccessDeniedReason }
  | { readonly ok: false; readonly reason: "unavailable"; readonly detail: string };
