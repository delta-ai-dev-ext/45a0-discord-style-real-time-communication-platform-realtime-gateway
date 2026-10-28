import { describe, expect, it } from "bun:test";
import { OutboundQueue } from "../../src/services/outbound-queue";
import { createFakeSocket } from "../mocks/fake-socket";

describe("bounded outbound queue", () => {
  it("writes frames immediately while the transport accepts them", () => {
    const socket = createFakeSocket();
    const queue = new OutboundQueue({ capacity: 4, sender: socket });

    expect(queue.push("a")).toBe(true);
    expect(queue.push("b")).toBe(true);
    expect(socket.sent).toEqual(["a", "b"]);
    expect(queue.size).toBe(0);
  });

  it("buffers while backpressured and resumes in order on drain", () => {
    const socket = createFakeSocket({ statuses: [-1] });
    const queue = new OutboundQueue({ capacity: 4, sender: socket });

    expect(queue.push("a")).toBe(true);
    expect(queue.isBackpressured).toBe(true);
    expect(queue.size).toBe(0);
    expect(queue.push("b")).toBe(true);
    expect(queue.size).toBe(1);

    queue.handleDrain();

    expect(queue.size).toBe(0);
    expect(queue.isBackpressured).toBe(false);
    expect(socket.sent).toEqual(["a", "b"]);
  });

  it("drops the session when the transport drops a frame", () => {
    const socket = createFakeSocket({ statuses: [0] });
    const overflows: OutboundQueue[] = [];
    const queue = new OutboundQueue({
      capacity: 4,
      sender: socket,
      onOverflow: (overflowed) => {
        overflows.push(overflowed);
        overflowed.closeWith('{"type":"reconnect","reason":"error"}', 1013, "overflow");
      },
    });

    expect(queue.push("a")).toBe(false);
    expect(overflows).toHaveLength(1);
    expect(queue.isClosed).toBe(true);
    expect(socket.closed).toEqual({ code: 1013, reason: "overflow" });
  });

  it("overflows when the buffer capacity is exceeded", () => {
    const socket = createFakeSocket({ statuses: [-1] });
    const queue = new OutboundQueue({
      capacity: 1,
      sender: socket,
      onOverflow: (overflowed) => overflowed.closeWith("reconnect"),
    });

    expect(queue.push("a")).toBe(true);
    expect(queue.push("b")).toBe(true);
    expect(queue.push("c")).toBe(false);
    expect(socket.sent).toEqual(["a", "reconnect"]);
    expect(queue.push("d")).toBe(false);
  });

  it("flushes pending frames before a graceful close", () => {
    const socket = createFakeSocket({ statuses: [-1] });
    const queue = new OutboundQueue({ capacity: 4, sender: socket });

    queue.push("a");
    queue.push("b");
    queue.closeNow(1001, "server shutdown");

    expect(socket.sent).toEqual(["a", "b"]);
    expect(socket.closed).toEqual({ code: 1001, reason: "server shutdown" });
    expect(queue.push("c")).toBe(false);
  });

  it("terminates abruptly and resolves drain waiters", async () => {
    const socket = createFakeSocket({ statuses: [-1] });
    const queue = new OutboundQueue({ capacity: 4, sender: socket });

    queue.push("a");
    const drained = queue.whenDrained();
    queue.terminate();

    await drained;
    expect(socket.terminated).toBe(true);
    expect(queue.isClosed).toBe(true);
  });

  it("resolves whenDrained immediately for an empty queue", async () => {
    const queue = new OutboundQueue({ capacity: 2, sender: createFakeSocket() });
    await queue.whenDrained();
    expect(queue.size).toBe(0);
  });

  it("closes the session when the transport throws on write", () => {
    const errors: unknown[] = [];
    const queue = new OutboundQueue({
      capacity: 2,
      sender: {
        send: () => {
          throw new Error("socket gone");
        },
        close: () => {},
        terminate: () => {},
      },
      onWriteError: (error) => errors.push(error),
    });

    expect(queue.push("a")).toBe(false);
    expect(errors).toHaveLength(1);
    expect(queue.isClosed).toBe(true);
  });

  it("reports drain waiters resolved after a flush", async () => {
    const socket = createFakeSocket({ statuses: [-1] });
    const queue = new OutboundQueue({ capacity: 4, sender: socket });

    queue.push("a");
    const drained = queue.whenDrained();
    queue.handleDrain();
    await drained;

    expect(queue.size).toBe(0);
  });
});
