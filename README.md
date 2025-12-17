# Meson ESG 数据处理工具

这个工具用于处理 MySQL 数据库中的 ESG 数据，并与区块链进行交互。

## 功能说明

1. 从 MySQL 数据库中分批读取未处理的 ESG 数据
2. 将数据转换为指定格式
3. 调用外部接口处理数据
4. 将返回的索引值写回数据库
5. 等待区块链交易确认
6. 继续处理下一批数据

## 安装依赖

```bash
npm install
```

## 配置

在 `.env` 文件中配置以下环境变量：

```env
DB_HOST=localhost
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database

CONFLUX_RPC_URL=https://main.confluxrpc.com
PRIVATE_KEY=your_private_key

API_ENDPOINT=http://localhost:3000/api/esg
API_KEY=your_api_key
```

### 配置说明

- `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`: MySQL 数据库连接配置
- `CONFLUX_RPC_URL`: Conflux 区块链 RPC 地址
- `PRIVATE_KEY`: 用于签名交易的私钥
- `API_ENDPOINT`: 外部 API 的地址
- `API_KEY`: 外部 API 的访问密钥

## 数据表结构

程序假设存在以下数据表：

```sql
DROP TABLE IF EXISTS `esg_block`;
CREATE TABLE `esg_block` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `date` varchar(15) DEFAULT NULL COMMENT '数据日期',
  `org_name` varchar(50) DEFAULT NULL COMMENT '组织名称',
  `projectname` varchar(255) DEFAULT NULL COMMENT '项目名称',
  `code` varchar(50) DEFAULT NULL COMMENT '碳编码',
  `index` int(11) DEFAULT NULL COMMENT '区块链索引',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `province_name` varchar(10) DEFAULT NULL COMMENT '省份简称',
  `cityname` varchar(255) DEFAULT NULL COMMENT '城市',
  `scope` varchar(10) DEFAULT NULL COMMENT '范围类别',
  `src` varchar(255) DEFAULT NULL COMMENT '碳排放来源',
  `activity_data` varchar(255) DEFAULT NULL COMMENT '活动数据',
  `carborn_emission_quantity` varchar(255) DEFAULT NULL COMMENT '碳排放当量',
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CODE` (`date`,`org_name`,`projectname`,`code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=565344 DEFAULT CHARSET=utf8;
```

## 运行程序

```bash
npm start
```

或者

```bash
node index.js
```

## 处理流程

1. 程序启动后会连接到数据库
2. 每次从数据库中读取 20 条未处理的数据（index 字段为 NULL 的记录）
3. 将每条数据转换为指定格式
4. 调用外部接口处理数据
5. 将返回的 index 值更新到对应的数据库记录中
6. 等待区块链交易确认（5个确认）
7. 继续处理下一批数据直到所有数据处理完毕

## 代码结构

- `index.js`: 主程序文件
- `.env`: 配置文件
- `package.json`: 项目依赖和脚本配置

## 注意事项

1. 外部接口调用部分目前是模拟实现，在生产环境中需要替换为真实的 API 调用代码
2. 区块链交易部分也需要根据实际的智能合约进行调整
3. 程序具有错误处理机制，单条数据处理失败不会影响其他数据的处理
4. 如果区块链交易长时间未确认，程序会记录错误并需要人工干预
5. 程序会自动等待区块链交易获得5个确认后再继续处理下一条数据