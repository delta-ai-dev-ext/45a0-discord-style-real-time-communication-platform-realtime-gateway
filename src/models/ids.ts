/**
 * Branded identifier types.
 *
 * Every id in this service is an opaque string that crosses a trust boundary
 * (client frames, Kafka envelopes, community_service responses). Branding keeps
 * a user id from being silently passed where a community id is expected without
 * paying for runtime wrappers.
 */

declare const identifierBrand: unique symbol;

type Brand<TValue, TName extends string> = TValue & {
  readonly [identifierBrand]: TName;
};

export type UserId = Brand<string, "UserId">;
export type CommunityId = Brand<string, "CommunityId">;
export type ChannelId = Brand<string, "ChannelId">;
export type ConnectionId = Brand<string, "ConnectionId">;
export type EventId = Brand<string, "EventId">;
export type TopicName = Brand<string, "TopicName">;

export const asUserId = (value: string): UserId => value as UserId;
export const asCommunityId = (value: string): CommunityId => value as CommunityId;
export const asChannelId = (value: string): ChannelId => value as ChannelId;
export const asConnectionId = (value: string): ConnectionId => value as ConnectionId;
export const asEventId = (value: string): EventId => value as EventId;
