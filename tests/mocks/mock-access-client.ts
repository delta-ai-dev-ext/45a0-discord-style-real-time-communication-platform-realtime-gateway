import type { AccessClient } from "../../src/gateways/access-client";
import type { AccessDecision, MemberAccessQuery } from "../../src/models/access";

export interface MockAccessClient extends AccessClient {
  readonly calls: MemberAccessQuery[];
  /** Answers keyed by `${communityId}|${userId}`. */
  readonly answers: Map<string, AccessDecision>;
  setAnswer(communityId: string, userId: string, decision: AccessDecision): void;
  allow(communityId: string, userId: string, visibleChannelIds: string[]): void;
  deny(communityId: string, userId: string): void;
  unavailable(communityId: string, userId: string): void;
}

const key = (communityId: string, userId: string) => `${communityId}|${userId}`;

/** In-memory `visible_channel_ids` oracle replacing community_service. */
export function createMockAccessClient(
  initial: Record<string, AccessDecision> = {},
): MockAccessClient {
  const answers = new Map(Object.entries(initial));
  const calls: MemberAccessQuery[] = [];

  return {
    calls,
    answers,
    setAnswer(communityId, userId, decision) {
      answers.set(key(communityId, userId), decision);
    },
    allow(communityId, userId, visibleChannelIds) {
      answers.set(key(communityId, userId), {
        ok: true,
        visibleChannelIds,
        timedOutUntil: null,
      });
    },
    deny(communityId, userId) {
      answers.set(key(communityId, userId), { ok: false, reason: "not_member" });
    },
    unavailable(communityId, userId) {
      answers.set(key(communityId, userId), {
        ok: false,
        reason: "unavailable",
        detail: "mock outage",
      });
    },
    async fetchAccess(query) {
      calls.push(query);
      return (
        answers.get(key(query.communityId, query.userId)) ?? {
          ok: false,
          reason: "not_member",
        }
      );
    },
  };
}
