require('dotenv').config();
const mysql = require('mysql2/promise');

// 数据库配置
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
};

// 分批处理的数量
const BATCH_SIZE = 20;

// 创建数据库连接
async function connectToDatabase() {
  try {
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
  } catch (error) {
    console.error('数据库连接失败:', error.message);
    throw error;
  }
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

module.exports = {
  connectToDatabase,
  getUnprocessedBatch,
  updateIndexField,
  BATCH_SIZE
};