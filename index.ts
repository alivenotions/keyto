import express from 'express'
import { DiskStorage } from './storage'

const app = express()

const PORT = process.env.PORT || 3000
let db: DiskStorage

app.get('/store/:key', async (req, res) => {
  try {
    const { key } = req.params
    console.log('finding key:', key)
    if (!key) {
      res.status(400).json({ error: 'key missing' })
      return
    }
    const record = await db.get(key)
    if (record) {
      const { timestamp, key, value } = record
      res.status(200).json({ timestamp, key, value })
      return
    }
    res.status(204).send()
  } catch (e) {
    console.log('error', e)
  }
})

app.post('/store', async (req, res) => {
  const { key, value } = req.query
  if (!key || !value) {
    res.status(400).json({ error: 'key or value missing' })
  }
  await db.set(key as string, value as string)
  res.status(201).send('ok')
})

app.listen(PORT, async () => {
  console.log('starting the db')
  db = await DiskStorage.init()
  console.log('db initialized')
  console.log(`listening on port: ${PORT}`)
})
