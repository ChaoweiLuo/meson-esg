require('dotenv').config();
const { connectToDatabase, getUnprocessedBatch, updateIndexField, BATCH_SIZE } = require('./database');
const { waitForTransaction } = require('./blockchain');
const { transformData, callExternalAPI } = require('./dataProcessor');

// 主处理函数（批量处理）
async function processBatch(connection, offset) {
  console.log(`正在处理偏移量 ${offset} 的数据批次...`);
  
  // 1. 获取未处理的数据批次
  const batch = await getUnprocessedBatch(connection, offset);
  
  if (batch.length === 0) {
    console.log('没有更多未处理的数据');
    return false; // 表示没有更多数据需要处理
  }
  
  console.log(`获取到 ${batch.length} 条数据`);
  
  try {
    // 2. 转换批次数据格式
    const transformedBatchData = batch.map(row => transformData(row));
    
    // 3. 调用外部接口（批量处理）
    console.log(`正在处理ID从 ${batch[0].id} 到 ${batch[batch.length - 1].id} 的数据批次...`);
    const apiResponse = await callExternalAPI(transformedBatchData);
    
    console.log('API响应:', JSON.stringify(apiResponse, null, 2));
    
    // 4. 等待交易确认
    const confirmed = await waitForTransaction(apiResponse.txHash);
    if (!confirmed) {
      console.error(`交易 ${apiResponse.txHash} 未能确认，需要人工检查`);
      // 这里可以添加更多的错误处理逻辑
      // 比如记录日志、发送通知等
      return false;
    }
    
    // 5. 批量更新数据库中的index字段
    if (apiResponse.entryIndex && apiResponse.entryIndex.length === batch.length) {
      // 构造批量更新的数据
      const updateData = batch.map((row, index) => ({
        id: row.id,
        index: apiResponse.entryIndex[index]
      }));
      
      // 执行批量更新
      await updateIndexField(connection, updateData);
      
      // 输出更新日志
      for (let i = 0; i < batch.length; i++) {
        const row = batch[i];
        const index = apiResponse.entryIndex[i];
        console.log(`已更新ID ${row.id} 的index为 ${index}`);
      }
    } else {
      console.error('API返回的entryIndex数组长度与批次数据不匹配');
      return false;
    }
    
    console.log(`批次数据处理完成，共处理 ${batch.length} 条记录`);
    
  } catch (error) {
    console.error(`处理批次数据时出错:`, error.message);
    // 可以选择继续处理下一批数据或停止处理
    // 根据具体需求决定是否继续
    return false;
  }
  
  return true; // 表示还有更多数据需要处理
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
    
    let offset = 0;
    let hasMoreData = true;
    
    // 循环处理数据批次
    while (hasMoreData) {
      hasMoreData = await processBatch(connection, offset);
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