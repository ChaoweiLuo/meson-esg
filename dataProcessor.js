require('dotenv').config();

// 外部API配置
const apiConfig = {
  endpoint: process.env.API_ENDPOINT || 'http://localhost:3000/api/esg',
  apiKey: process.env.API_KEY || ''
};

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

// 调用外部API（批量处理）
async function callExternalAPI(batchData) {
  console.log('调用外部接口，发送批量数据:', JSON.stringify(batchData, null, 2));
  
  // 实际实现中，这里应该调用真实的外部API
  // 示例代码：
  /*
  const response = await fetch(apiConfig.endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiConfig.apiKey}`
    },
    body: JSON.stringify(batchData)
  });
  
  if (!response.ok) {
    throw new Error(`API调用失败: ${response.status} ${response.statusText}`);
  }
  
  const result = await response.json();
  return result;
  */
  
  // 当前为模拟实现
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  // 模拟返回值
  return {
    txHash: `0x${Math.random().toString(16).substr(2, 64)}`,
    entryIndex: Array.from({length: batchData.length}, () => Math.floor(Math.random() * 1000000)),
    count: batchData.length
  };
}

module.exports = {
  transformData,
  callExternalAPI
};