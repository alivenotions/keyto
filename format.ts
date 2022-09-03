import { Buffer } from 'node:buffer'

const TIMESTAMP_SIZE = 4
const KEY_SIZE = 4
const VALUE_SIZE = 4
const HEADER_SIZE = TIMESTAMP_SIZE + KEY_SIZE + VALUE_SIZE

// function encodeKv(timestamp: number, key: string, value: string): [number, Buffer] {
// }
//
// function decodeKv(data: Buffer): [number, string, string] {
// }

export function encodeHeader(timestamp: number, keySize: number, valueSize: number): Buffer {
  const buffer = Buffer.allocUnsafe(HEADER_SIZE)

  buffer.writeUInt32LE(timestamp)
  buffer.writeUInt32LE(keySize, TIMESTAMP_SIZE)
  buffer.writeUInt32LE(valueSize, TIMESTAMP_SIZE + KEY_SIZE)

  return buffer
}
export function decodeHeader(buffer: Buffer): [number, number, number] {
  const timestamp = buffer.subarray(0, TIMESTAMP_SIZE).readUInt32LE()
  const keySize = buffer.readUInt32LE(TIMESTAMP_SIZE)
  const valueSize = buffer.readUInt32LE(TIMESTAMP_SIZE + KEY_SIZE)

  return [timestamp, keySize, valueSize]
}
