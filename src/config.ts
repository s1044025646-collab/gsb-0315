export const LIMITS = {
  MAX_QUANTUM: 1_000_000,
  MAX_PACKET_LENGTH: 1_000_000,
  MAX_PACKETS_PER_RUN: 100_000,
  MAX_ADVANCE_PER_CALL: 10_000,
  MAX_STREAMS_PER_RUN: 10_000,
  MAX_KEY_LENGTH: 128,
  // 额度上限，防止无限积压流累加出过大整数（仍远小于 Number.MAX_SAFE_INTEGER）
  MAX_DEFICIT: 2 ** 40,
} as const;

export type ErrorCode =
  | "INVALID_ARGUMENT"
  | "RUN_NOT_FOUND"
  | "STREAM_NOT_FOUND"
  | "PACKET_NOT_FOUND"
  | "EVENT_NOT_FOUND"
  | "DUPLICATE_STREAM"
  | "DUPLICATE_PACKET"
  | "RUN_SEALED"
  | "LIMIT_EXCEEDED"
  | "NO_ACTIVE_STREAM";

export class DrrError extends Error {
  constructor(public code: ErrorCode, message: string, public status = 400) {
    super(message);
  }
}

export function assertInt(value: unknown, name: string, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new DrrError("INVALID_ARGUMENT", `${name} 必须是 [${min}, ${max}] 范围内的安全整数`);
  }
  return value;
}

export function assertKey(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > LIMITS.MAX_KEY_LENGTH) {
    throw new DrrError("INVALID_ARGUMENT", `${name} 必须是 1..${LIMITS.MAX_KEY_LENGTH} 字符的字符串`);
  }
  return value;
}
