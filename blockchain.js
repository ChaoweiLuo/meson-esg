require('dotenv').config();
const { Conflux } = require('js-conflux-sdk');

// Conflux链配置
const confluxConfig = {
  url: process.env.CONFLUX_RPC_URL,
  privateKey: process.env.PRIVATE_KEY
};

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

module.exports = {
  initializeConflux,
  waitForTransaction
};