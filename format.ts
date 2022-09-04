import { Buffer } from 'node:buffer'

const TIMESTAMP_SIZE = 4
const KEY_SIZE = 4
const VALUE_SIZE = 4
const HEADER_SIZE = TIMESTAMP_SIZE + KEY_SIZE + VALUE_SIZE

export function encodeKv(
  timestamp: number,
  key: string,
  value: string
): Buffer {
  const header = encodeHeader(timestamp, key.length, value.length)
  const data = Buffer.concat([header, Buffer.from(key), Buffer.from(value)])
  return data
}

export function decodeKv(buffer: Buffer): [number, string, string] {
  const [timestamp, keySize, _valueSize] = decodeHeader(
    buffer.subarray(0, HEADER_SIZE)
  )
  const keyBytes = buffer.subarray(HEADER_SIZE, HEADER_SIZE + keySize)
  const valueBytes = buffer.subarray(HEADER_SIZE + keySize)

  const key = keyBytes.toString('utf8')
  const value = valueBytes.toString('utf8')

  return [timestamp, key, value]
}

export function encodeHeader(
  timestamp: number,
  keySize: number,
  valueSize: number
): Buffer {
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
