import { encodeHeader, decodeHeader, encodeKv, decodeKv } from './format'

describe('format', () => {
  describe('headers', () => {
    it.each([
      [1662249600, 12312, 234],
      [1567555200, 23, 32142543],
      // this is 2099
      [4092163200, 1, 1234],
    ])('should encode correctly', (timestampInSecs, keySize, valueSize) => {
      const data = encodeHeader(timestampInSecs, keySize, valueSize)
      const header = decodeHeader(data)
      expect(header).toEqual([timestampInSecs, keySize, valueSize])
    })

    it.each([
      [2 ** 32, 1, 1],
      [1, 2 ** 32, 1],
      [1, 1, 2 ** 32],
    ])(
      'should throw if the size is > 4 bytes',
      (timestampInSecs, keySize, valueSize) => {
        expect(() => {
          encodeHeader(timestampInSecs, keySize, valueSize)
        }).toThrow()
      }
    )
  })

  describe('key value', () => {
    it.each([
      [1662249600, 'hello', 'world'],
      [1567555200, 'name', 'veryveryveryvery long name'],
      [4092163200, 'quality', 'likes to play all day long and all day night'],
      [0, '', ''],
    ])('should encode key values correctly', (timestampInSecs, key, value) => {
      const [_size, buffer] = encodeKv(timestampInSecs, key, value)
      const data = decodeKv(buffer)
      expect(data).toEqual([timestampInSecs, key, value])
    })
  })
})
