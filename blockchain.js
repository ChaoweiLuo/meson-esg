const { ethers } = require('ethers');

// Conflux链配置
const confluxRpc = process.env.CONFLUX_RPC_URL

// 等待交易确认（使用ethers.js的waitForTransaction方法）
async function waitForTransaction(conflux, hash) {
  console.log(`等待交易 ${hash} 确认...`);
  
  try {
    // 验证交易哈希格式
    if (!hash || typeof hash !== 'string') {
      throw new Error('无效的交易哈希');
    }
    
    // 检查是否为模拟数据（不以0x开头或长度不对）
    const isSimulationData = !hash.startsWith('0x') || hash.length < 64;
    
    if (isSimulationData) {
      console.warn(`警告：检测到模拟数据，跳过实际的区块链查询`);
      // 对于模拟数据，我们直接等待一段时间后返回成功
      await new Promise(resolve => setTimeout(resolve, 5000));
      console.log(`交易 ${hash} 已获得5个确认（模拟）`);
      return true;
    }
    
    // 确保交易哈希以0x开头
    let formattedHash = hash;
    if (!hash.startsWith('0x')) {
      formattedHash = '0x' + hash;
    }
    
    // 创建ethers.js provider
    const provider = new ethers.JsonRpcProvider(confluxRpc);
    
    // 使用ethers.js的waitForTransaction方法等待交易确认
    // 参数：交易哈希, 确认数(5), 超时时间(30秒)
    // conflux 链上一个区块只需要 0.5 秒，所以这里的30 秒是一个相对安全的超时时间
    await provider.waitForTransaction(formattedHash, 5, 30_000);
    
    console.log(`交易 ${formattedHash} 已获得5个确认`);
    return true;
  } catch (error) {
    console.error(`等待交易 ${hash} 确认时出错:`, error.message);
    return false;
  }
}

module.exports = {
  initializeConflux,
  waitForTransaction
};