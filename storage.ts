import fs from 'node:fs/promises'
import { existsSync, fsyncSync } from 'node:fs'
import { Buffer } from 'node:buffer'
import { decodeKv, encodeKv, decodeHeader, HEADER_SIZE } from './format'

type KeyValueRecord = {
  timestamp: number
  position: number
  size: number
}

export class DiskStorage {
  fileName: string
  writePosition = 0
  file: fs.FileHandle | null = null
  store = new Map<string, KeyValueRecord>()

  private constructor(fileName: string, file: fs.FileHandle | null) {
    this.fileName = fileName
    this.file = file
  }

  static async init(fileName = 'data.db') {
    let file = null
    const diskStorage = new DiskStorage(fileName, file)

    if (existsSync(fileName)) {
      console.log('file already exists')
      await diskStorage.initializeStore()
    }
    diskStorage.file = await fs.open(fileName, 'a+')
    return diskStorage
  }

  private async initializeStore() {
    let file: fs.FileHandle
    try {
      file = await fs.open(this.fileName, 'r')
    } catch (e) {
      console.error(
        'this should not fail since, we already checked that this file exists'
      )
      throw new Error('File not found error')
    }

    console.log('initializing store')
    console.log('this will take a long time if the db is large')
    const buffer = await fs.readFile(file)

    let offset = 0
    while (offset < buffer.length) {
      const [timestamp, keySize, valueSize] = decodeHeader(
        buffer.subarray(offset, offset + HEADER_SIZE)
      )
      const size = HEADER_SIZE + keySize + valueSize
      const [_timestamp, key, _value] = decodeKv(
        buffer.subarray(offset, offset + size)
      )
      console.log(`read: ${key}`)
      this.store.set(key, { timestamp, position: offset, size })
      offset += size
    }
    console.log('finished initializing the store')
    this.writePosition += offset

    await file.close()
  }

  public async set(key: string, value: string) {
    const timestampInSecs = Math.floor(Date.now() / 1000)
    const [size, data] = encodeKv(timestampInSecs, key, value)
    if (!this.file) {
      throw new Error('File not found')
    }
    const buffer = Buffer.from(data)
    await this.write(buffer)
    this.store.set(key, {
      timestamp: timestampInSecs,
      position: this.writePosition,
      size,
    })

    this.writePosition += size
  }

  public async get(key: string) {
    const record = this.store.get(key)

    if (!this.file) {
      throw new Error('File not found')
    }
    if (!record) {
      return ''
    }

    const buffer = Buffer.allocUnsafe(record.size)
    await this.file.read(buffer, null, record.size, record.position)
    const [timestamp, _key, value] = decodeKv(buffer)

    return { timestamp, key, value }
  }

  private async write(data: Buffer) {
    if (!this.file) {
      throw new Error('File not found')
    }
    await this.file.write(data)
    fsyncSync(this.file.fd)
  }

  public close() {
    if (this.file) {
      fsyncSync(this.file.fd)
      this.file.close()
    }
  }
}
