import { encodeHeader, decodeHeader } from './format'

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
    ])('should throw if the size is > 4 bytes', (timestampInSecs, keySize, valueSize) => {
      expect(() => {
        encodeHeader(timestampInSecs, keySize, valueSize)
      }).toThrow()
    })
  })
})
