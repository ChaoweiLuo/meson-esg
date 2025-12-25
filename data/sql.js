import { createReadStream } from 'fs'
import { join, dirname } from 'path'
import { createInterface } from 'readline'
import { createPool } from 'mysql2/promise'
import { config } from 'dotenv'
import { fileURLToPath } from 'url'

config()

/**
 * 配置区
 */
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const SQL_FILE = join(__dirname, 'esg_block_202507_202511.sql')
const BATCH_SIZE = 500   // 每批 500 条（可调）
const TABLE_NAME = 'esg_block'

// TODO: 初始化数据到数据库中
const db = await createPool({
  host: '127.0.0.1',
  user: 'root',
  password: '123456',
  database: 'Meson-Test',
  charset: 'utf8mb4',
  waitForConnections: true,
  connectionLimit: 5
})

/**
 * 状态变量
 */
let buffer = []
let insertPrefix = ''
let total = 0

async function flush () {
  if (buffer.length === 0) return

  const sql = `
    ${insertPrefix}
    ${buffer.join(',\n')}
  `

  await db.query(sql)

  total += buffer.length
  console.log(`✅ 已导入 ${total} 条`)

  buffer = []
}

/**
 * 主流程
 */
async function run () {
  const rl = createInterface({
    input: createReadStream(SQL_FILE),
    crlfDelay: Infinity
  })

  for await (const line of rl) {
    const l = line.trim()
    if (!l) continue

    // 1️⃣ 捕获 INSERT 头
    if (l.startsWith('INSERT INTO')) {
      insertPrefix = l.replace(/VALUES\s*$/i, 'VALUES')
      continue
    }

    // 2️⃣ 捕获 VALUES 行
    if (l.startsWith('(')) {
      // 去掉结尾的 , 或 ;
      const valueLine = l.replace(/[,;]$/, '')
      buffer.push(valueLine)

      if (buffer.length >= BATCH_SIZE) {
        await flush()
      }
    }
  }

  // 3️⃣ 处理剩余数据
  await flush()
  await db.end()

  console.log('🎉 导入完成')
}

run().catch(err => {
  console.error('❌ 导入失败:', err)
  process.exit(1)
})
