export type ErrorCode =
  | "INVALID_ARGUMENT"
  | "UNKNOWN_RUN"
  | "UNKNOWN_STREAM"
  | "UNKNOWN_PACKET"
  | "DUPLICATE_STREAM"
  | "DUPLICATE_PACKET"
  | "LIMIT_EXCEEDED"
  | "NO_ACTIVE_STREAMS"
  | "STREAMS_FROZEN"
  | "NOT_FOUND";

export class DrrError extends Error {
  constructor(
    public code: ErrorCode,
    message: string,
    public httpStatus = 400
  ) {
    super(message);
  }
}

export const LIMITS = {
  MAX_QUANTUM: 1_000_000,
  MAX_PACKET_LENGTH: 1_000_000,
  MAX_PACKETS_PER_RUN: 100_000,
  MAX_VISITS_PER_ADVANCE: 10_000,
  MAX_ID_LENGTH: 128,
} as const;

export function assertSafeInt(value: unknown, name: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new DrrError("INVALID_ARGUMENT", `${name} 必须是安全整数`);
  }
}

export function assertPositiveInt(value: unknown, name: string, max: number): asserts value is number {
  assertSafeInt(value, name);
  if ((value as number) <= 0) {
    throw new DrrError("INVALID_ARGUMENT", `${name} 必须是正整数`);
  }
  if ((value as number) > max) {
    throw new DrrError("LIMIT_EXCEEDED", `${name} 超过上限 ${max}`);
  }
}

export function assertId(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > LIMITS.MAX_ID_LENGTH) {
    throw new DrrError("INVALID_ARGUMENT", `${name} 必须是 1..${LIMITS.MAX_ID_LENGTH} 字符的字符串`);
  }
}
