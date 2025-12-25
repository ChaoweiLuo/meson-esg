import { createReadStream } from 'fs'
import { join, dirname } from 'path'
import { createInterface } from 'readline'
import { MongoClient, ServerApiVersion } from 'mongodb'
import { config } from 'dotenv'
import { fileURLToPath } from 'url'

config()

/**
 * 配置区
 */
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const SQL_FILE = join(__dirname, 'esg_block_2024.sql')
const BATCH_SIZE = 500   // 每批 500 条（可调）
const COLLECTION_NAME = 'esg_blocks'

// MongoDB连接配置
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017'
const DATABASE_NAME = process.env.MONGODB_DATABASE || 'meson-testnet'

/**
 * 状态变量
 */
let buffer = []
let insertPrefix = ''
let total = 0
let processed = 0  // 已处理的总行数
let failed = 0       // 失败的记录数

// 连接到MongoDB
const client = new MongoClient(MONGODB_URI, {
  serverSelectionTimeoutMS: 5000, // 5秒超时
  maxPoolSize: 10, // 连接池大小
})
let db, collection

async function connectDB() {
  try {
    await client.connect()
    console.log('✅ 连接到MongoDB')
    db = client.db(DATABASE_NAME)
    collection = db.collection(COLLECTION_NAME)
    
    // 确保集合存在
    await db.createCollection(COLLECTION_NAME).catch(() => {})
    
    console.log(`📋 使用数据库: ${DATABASE_NAME}, 集合: ${COLLECTION_NAME}`)
  } catch (err) {
    console.error('❌ MongoDB连接失败:', err)
    process.exit(1)
  }
}

// 解析SQL值行并转换为MongoDB文档
function parseSQLValues(valueLine) {
  try {
    // 移除首尾括号
    let content = valueLine.trim()
    if (content.startsWith('(')) content = content.substring(1)
    if (content.endsWith(')')) content = content.slice(0, -1)
    
    // 分割值，注意处理引号内的逗号
    const values = []
    let current = ''
    let inQuotes = false
    let quoteChar = null
    
    for (let i = 0; i < content.length; i++) {
      const char = content[i]
      
      if ((char === '"' || char === "'") && (i === 0 || content[i-1] !== '\\')) {
        if (!inQuotes) {
          inQuotes = true
          quoteChar = char
        } else if (char === quoteChar) {
          inQuotes = false
          quoteChar = null
        }
        current += char
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim())
        current = ''
      } else {
        current += char
      }
    }
    
    if (current.trim()) {
      values.push(current.trim())
    }
    
    // 转换值并创建文档对象
    // 根据MySQL表结构映射字段: id, date, org_name, projectname, code, index, created_at, updated_at, province_name, cityname, scope, src, activity_data, carborn_emission_quantity
    if (values.length < 14) {
      console.error('⚠️  SQL值数量不足，期望14个字段，实际:', values.length, '值:', valueLine)
      failed++
      return null
    }
    
    const doc = {}
    
    // 映射MySQL表结构到MongoDB文档
    for (let i = 0; i < values.length; i++) {
      let value = values[i].trim()
      
      // 移除引号
      if ((value.startsWith('"') && value.endsWith('"')) || 
          (value.startsWith("'") && value.endsWith("'"))) {
        value = value.substring(1, value.length - 1)
      }
      
      // 尝试转换数字和布尔值
      if (value === 'NULL' || value === 'null' || value === '') {
        value = null
      } else if (!isNaN(value) && value.trim() !== '') {
        // 检查是否为整数还是浮点数
        value = value.includes('.') ? parseFloat(value) : parseInt(value, 10)
      } else if (value === 'true' || value === 'false') {
        value = value === 'true'
      }
      
      // 根据字段索引映射到正确的字段名
      switch(i) {
        case 0:  // id
          doc._id = value
          break
        case 1:  // date
          doc.date = value
          break
        case 2:  // org_name
          doc.orgName = value
          break
        case 3:  // projectname
          doc.projectName = value
          break
        case 4:  // code
          doc.code = value
          break
        case 5:  // index
          doc.index = value
          break
        case 6:  // created_at
          doc.createdAt = value ? new Date(value) : null
          break
        case 7:  // updated_at
          doc.updatedAt = value ? new Date(value) : null
          break
        case 8:  // province_name
          doc.provinceName = value
          break
        case 9:  // cityname
          doc.cityName = value
          break
        case 10: // scope
          doc.scope = value
          break
        case 11: // src
          doc.src = value
          break
        case 12: // activity_data
          doc.activityData = value
          break
        case 13: // carborn_emission_quantity
          doc.carbonEmissionQuantity = value
          break
        default:
          // 对于超过14个字段的情况，使用通用字段名
          doc[`field_${i}`] = value
      }
    }
    
    return doc
  } catch (err) {
    console.error('❌ 解析SQL值失败:', err, '值:', valueLine)
    failed++
    return null
  }
}

async function flush() {
  if (buffer.length === 0) return

  try {
    // 解析所有值行并转换为MongoDB文档
    const documents = []
    for (const valueLine of buffer) {
      const doc = parseSQLValues(valueLine)
      if (doc) {
        documents.push(doc)
      }
    }
    
    if (documents.length > 0) {
      // 批量插入到MongoDB
      await collection.insertMany(documents)
      
      total += documents.length
      console.log(`✅ 已导入 ${total} 条 (已处理: ${processed}, 失败: ${failed})`)
    } else {
      console.log(`⚠️  批量处理中没有有效文档 (已处理: ${processed}, 失败: ${failed})`)
    }
    
    buffer = []
  } catch (err) {
    console.error('❌ 批量插入失败:', err)
    
    // 如果整个批次失败，尝试逐个插入
    console.log('⚠️  尝试逐个插入文档...')
    for (const valueLine of buffer) {
      try {
        const doc = parseSQLValues(valueLine)
        if (doc) {
          await collection.insertOne(doc)
          total++
        }
      } catch (singleErr) {
        console.error('❌ 单个文档插入失败:', singleErr)
        failed++
      }
    }
    
    console.log(`✅ 逐个插入完成 (已导入: ${total}, 已处理: ${processed}, 失败: ${failed})`)
    
    buffer = []
  }
}

/**
 * 主流程
 */
async function run() {
  console.log('🚀 开始导入SQL数据到MongoDB...')
  await connectDB()
  
  const rl = createInterface({
    input: createReadStream(SQL_FILE),
    crlfDelay: Infinity
  })

  for await (const line of rl) {
    const l = line.trim()
    if (!l) continue

    processed++

    // 1️⃣ 捕获 INSERT 头以了解表结构
    if (l.startsWith('INSERT INTO')) {
      insertPrefix = l.replace(/VALUES\s*$/i, 'VALUES')
      console.log(`📋 检测到插入表: ${COLLECTION_NAME}`)
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
  await client.close()

  console.log(`🎉 导入完成! 总计: ${total}, 失败: ${failed}, 已处理: ${processed}`)
}

// 添加信号处理器以优雅地处理中断
process.on('SIGINT', async () => {
  console.log('\n⚠️  接收到中断信号，正在关闭连接...')
  if (client) {
    await client.close()
  }
  console.log('✅ 连接已关闭')
  process.exit(0)
})

run().catch(err => {
  console.error('❌ 导入失败:', err)
  
  // 确保在错误情况下也关闭连接
  client.close().catch(closeErr => {
    console.error('❌ 关闭数据库连接时出错:', closeErr)
  })
  
  process.exit(1)
})