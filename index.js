require('dotenv').config();
const mysql = require('mysql2/promise');
// 使用官方的 Conflux SDK
const { Conflux } = require('js-conflux-sdk');

// 数据库配置
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
};

// Conflux链配置
const confluxConfig = {
  url: process.env.CONFLUX_RPC_URL,
  privateKey: process.env.PRIVATE_KEY
};

// 外部API配置
const apiConfig = {
  endpoint: process.env.API_ENDPOINT || 'http://localhost:3000/api/esg',
  apiKey: process.env.API_KEY || ''
};

// 分批处理的数量
const BATCH_SIZE = 20;

// 创建数据库连接
async function connectToDatabase() {
  // 创建连接池
  const pool = mysql.createPool({
    host: dbConfig.host,
    user: dbConfig.user,
    password: dbConfig.password,
    database: dbConfig.database,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });
  
  return pool;
}

// 获取未处理的数据批次
async function getUnprocessedBatch(connection, offset) {
  try {
    // 确保 offset 和 BATCH_SIZE 都是数字类型，并且不为负数
    const limit = Math.max(1, parseInt(BATCH_SIZE) || 20);
    const offsetValue = Math.max(0, parseInt(offset) || 0);
    
    // 使用字符串拼接方式构建查询语句，避免参数绑定问题
    const query = `
      SELECT * FROM esg_block 
      WHERE \`index\` IS NULL 
      ORDER BY id ASC 
      LIMIT ${limit} OFFSET ${offsetValue}
    `;
    
    const [rows] = await connection.execute(query);
    return rows;
  } catch (error) {
    console.error('查询数据库时出错:', error.message);
    throw error;
  }
}

// 转换数据格式
function transformData(row) {
  return {
    dataTime: Math.floor(new Date(row.created_at).getTime() / 1000),
    data: {
      prov: row.province_name || '',
      city: row.cityname || '',
      unit: row.org_name || '',
      proj: row.projectname || '',
      cat: row.scope || '',
      code: row.code || '',
      src: row.src || '',
      act_data: row.activity_data || '',
      co2e: row.carborn_emission_quantity || ''
    }
  };
}

// 调用外部API
async function callExternalAPI(data) {
  console.log('调用外部接口，发送数据:', JSON.stringify(data, null, 2));
  
  // 实际实现中，这里应该调用真实的外部API
  // 示例代码：
  /*
  const response = await fetch(apiConfig.endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiConfig.apiKey}`
    },
    body: JSON.stringify(data)
  });
  
  if (!response.ok) {
    throw new Error(`API调用失败: ${response.status} ${response.statusText}`);
  }
  
  const result = await response.json();
  return result.index;
  */
  
  // 当前为模拟实现
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  // 模拟返回index值
  return Math.floor(Math.random() * 1000000);
}

// 更新数据库中的index字段
async function updateIndexField(connection, id, index) {
  const query = 'UPDATE esg_block SET `index` = ? WHERE id = ?';
  try {
    // 确保 id 和 index 都是数字类型
    const idValue = parseInt(id);
    const indexValue = parseInt(index);
    
    if (isNaN(idValue) || isNaN(indexValue)) {
      throw new Error(`无效的参数: id=${id}, index=${index}`);
    }
    
    await connection.execute(query, [indexValue, idValue]);
  } catch (error) {
    console.error('更新数据库时出错:', error.message);
    throw error;
  }
}

// 等待交易确认（Conflux链）
async function waitForTransaction(conflux, hash) {
  console.log(`等待交易 ${hash} 确认...`);
  
  try {
    // 简化的等待逻辑，实际使用时可以根据需要调整
    // 这里只是模拟等待过程
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    console.log(`交易 ${hash} 已获得5个确认`);
    return true;
  } catch (error) {
    console.error(`等待交易 ${hash} 确认时出错:`, error.message);
    return false;
  }
}

// 主处理函数
async function processBatch(connection, conflux, offset) {
  console.log(`正在处理偏移量 ${offset} 的数据批次...`);
  
  // 1. 获取未处理的数据批次
  const batch = await getUnprocessedBatch(connection, offset);
  
  if (batch.length === 0) {
    console.log('没有更多未处理的数据');
    return false; // 表示没有更多数据需要处理
  }
  
  console.log(`获取到 ${batch.length} 条数据`);
  
  // 2. 处理每条数据
  for (const row of batch) {
    try {
      // 转换数据格式
      const transformedData = transformData(row);
      
      // 调用外部接口
      console.log(`正在处理ID为 ${row.id} 的数据...`);
      const index = await callExternalAPI(transformedData);
      
      // 更新数据库中的index字段
      await updateIndexField(connection, row.id, index);
      console.log(`已更新ID ${row.id} 的index为 ${index}`);
      
      // 生成模拟交易哈希（实际应用中应从区块链操作中获得）
      const transactionHash = `0x${Math.random().toString(16).substr(2, 64)}`;
      
      // 等待交易确认
      const confirmed = await waitForTransaction(conflux, transactionHash);
      if (!confirmed) {
        console.error(`交易 ${transactionHash} 未能确认，需要人工检查`);
        // 这里可以添加更多的错误处理逻辑
        // 比如记录日志、发送通知等
      }
      
    } catch (error) {
      console.error(`处理ID为 ${row.id} 的数据时出错:`, error.message);
      // 可以选择继续处理下一条数据或停止处理
      // 根据具体需求决定是否继续
    }
  }
  
  return true; // 表示还有更多数据需要处理
}

// 初始化Conflux客户端
function initializeConflux() {
  try {
    // 使用官方的 js-conflux-sdk
    const conflux = new Conflux({
      url: confluxConfig.url,
      // 根据需要添加其他配置
    });
    
    return conflux;
  } catch (error) {
    console.error('初始化Conflux客户端时出错:', error.message);
    return null;
  }
}

// 主函数
async function main() {
  let connection;
  
  try {
    // 连接数据库
    connection = await connectToDatabase();
    console.log('数据库连接成功');
    
    // 测试数据库连接
    await connection.execute('SELECT 1');
    
    // 初始化Conflux客户端
    const conflux = initializeConflux();
    console.log('Conflux客户端初始化成功');
    
    let offset = 0;
    let hasMoreData = true;
    
    // 循环处理数据批次
    while (hasMoreData) {
      hasMoreData = await processBatch(connection, conflux, offset);
      offset += BATCH_SIZE;
      
      // 在处理下一个批次前稍作延迟，避免过于频繁的请求
      if (hasMoreData) {
        console.log('准备处理下一批数据...');
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    console.log('所有数据处理完成');
    
  } catch (error) {
    console.error('程序执行出错:', error.message);
    // 如果有错误堆栈信息，也一并输出
    if (error.stack) {
      console.error(error.stack);
    }
  } finally {
    // 关闭数据库连接
    if (connection) {
      await connection.end();
      console.log('数据库连接已关闭');
    }
  }
}

// 执行主函数
main();