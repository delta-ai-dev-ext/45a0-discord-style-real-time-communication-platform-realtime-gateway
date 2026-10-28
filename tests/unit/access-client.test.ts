import { describe, expect, it } from "bun:test";
import { createManualClock } from "../../src/lib/clock";
import { silentLogger } from "../../src/lib/logger";
import { MetricsRegistry } from "../../src/lib/metrics";
import { HttpAccessClient, type FetchLike } from "../../src/gateways/access-client";

const TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const QUERY = { communityId: "c1", userId: "u1", traceparent: TRACEPARENT };

interface Call {
  readonly url: string;
  readonly headers: Record<string, string>;
}

function setup(responder: (call: Call, attempt: number) => Promise<Response> | Response) {
  const calls: Call[] = [];
  const fetchImpl: FetchLike = async (url, init) => {
    const call = { url, headers: { ...(init?.headers ?? {}) } };
    calls.push(call);
    return responder(call, calls.length);
  };

  const metrics = new MetricsRegistry();
  const client = new HttpAccessClient({
    baseUrl: "http://community_service:3000/",
    token: "internal-token",
    timeoutMs: 300,
    clock: createManualClock(),
    metrics,
    logger: silentLogger,
    fetchImpl,
  });

  return { client, calls, metrics };
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

const accessBody = {
  community_id: "c1",
  user_id: "u1",
  is_member: true,
  banned: false,
  visible_channel_ids: ["ch1", "ch2"],
};

describe("community_service access client", () => {
  it("calls the internal endpoint with the bearer token and traceparent", async () => {
    const { client, calls } = setup(() => json(accessBody));

    const decision = await client.fetchAccess(QUERY);

    expect(decision).toEqual({
      ok: true,
      visibleChannelIds: ["ch1", "ch2"],
      timedOutUntil: null,
    });
    expect(calls[0]?.url).toBe(
      "http://community_service:3000/internal/v1/communities/c1/members/u1/access",
    );
    expect(calls[0]?.headers.authorization).toBe("Bearer internal-token");
    expect(calls[0]?.headers.traceparent).toBe(TRACEPARENT);
    expect(calls).toHaveLength(1);
  });

  it("treats banned and non-member answers as authoritative denials", async () => {
    const banned = setup(() => json({ ...accessBody, banned: true }));
    const notMember = setup(() => json({ ...accessBody, is_member: false }));

    expect(await banned.client.fetchAccess(QUERY)).toEqual({ ok: false, reason: "banned" });
    expect(await notMember.client.fetchAccess(QUERY)).toEqual({
      ok: false,
      reason: "not_member",
    });
    expect(banned.calls).toHaveLength(1);
  });

  it("maps 404 to not_member without retrying", async () => {
    const { client, calls } = setup(() => new Response("missing", { status: 404 }));

    expect(await client.fetchAccess(QUERY)).toEqual({ ok: false, reason: "not_member" });
    expect(calls).toHaveLength(1);
  });

  it("retries exactly once on a 5xx and then succeeds", async () => {
    const { client, calls } = setup((_call, attempt) =>
      attempt === 1 ? new Response("boom", { status: 503 }) : json(accessBody),
    );

    expect((await client.fetchAccess(QUERY)).ok).toBe(true);
    expect(calls).toHaveLength(2);
  });

  it("fails closed after one retry on a persistent 5xx", async () => {
    const { client, calls } = setup(() => new Response("boom", { status: 500 }));
    const decision = await client.fetchAccess(QUERY);

    expect(decision).toMatchObject({ ok: false, reason: "unavailable" });
    expect(calls).toHaveLength(2);
  });

  it("fails closed when the internal token is rejected", async () => {
    const { client, calls } = setup(() => new Response("unauthorized", { status: 401 }));

    expect(await client.fetchAccess(QUERY)).toMatchObject({ ok: false, reason: "unavailable" });
    expect(calls).toHaveLength(2);
  });

  it("fails closed on a timeout or network error", async () => {
    const timeout = setup(() => {
      throw new DOMException("timed out", "TimeoutError");
    });
    const network = setup(() => {
      throw new TypeError("fetch failed");
    });

    expect(await timeout.client.fetchAccess(QUERY)).toMatchObject({
      ok: false,
      reason: "unavailable",
    });
    expect(timeout.calls).toHaveLength(2);
    expect(await network.client.fetchAccess(QUERY)).toMatchObject({
      ok: false,
      reason: "unavailable",
    });
  });

  it("fails closed on an unparseable payload", async () => {
    const { client } = setup(() => json({ unexpected: true }));

    expect(await client.fetchAccess(QUERY)).toMatchObject({ ok: false, reason: "unavailable" });
  });

  it("records access-fetch duration samples per attempt", async () => {
    const { client, metrics } = setup(() => json(accessBody));
    await client.fetchAccess(QUERY);

    const rendered = await metrics.registry.metrics();
    expect(rendered).toContain("gateway_access_fetch_duration_seconds_count 1");
  });

  it("supports a single attempt configuration", async () => {
    const metrics = new MetricsRegistry();
    const calls: string[] = [];
    const client = new HttpAccessClient({
      baseUrl: "http://community_service:3000",
      token: "t",
      timeoutMs: 10,
      clock: createManualClock(),
      metrics,
      logger: silentLogger,
      attempts: 1,
      fetchImpl: async (url) => {
        calls.push(url);
        return new Response("boom", { status: 502 });
      },
    });

    expect(await client.fetchAccess(QUERY)).toMatchObject({ ok: false, reason: "unavailable" });
    expect(calls).toHaveLength(1);
  });
});
