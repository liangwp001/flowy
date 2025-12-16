# 🌊 Flowy

<div align="center">

**一个现代化的 Python 工作流管理框架，带有强大的 Web 管理界面**

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Framework](https://img.shields.io/badge/framework-Flask-red.svg)](https://flask.palletsprojects.com/)

[功能特性](#-功能特性) • [快速开始](#-快速开始) • [安装方法](#-安装方法) • [使用场景](#-使用场景) • [Web界面](#-web界面) • [API文档](#-api文档)

</div>

---

## 🚀 核心价值

Flowy 是一个专为 Python 开发者设计的轻量级工作流管理框架，让复杂的业务流程变得简单易管理：

- **🎯 简单易用** - 仅需几行代码即可定义复杂工作流
- **📊 可视化管理** - 现代化 Web 界面，实时监控执行状态
- **⏰ 定时调度** - 支持 Cron 表达式的灵活任务调度
- **📝 完整日志** - 详细的执行日志和错误追踪
- **🔄 自动重试** - 内置错误处理和重试机制
- **📈 统计分析** - 丰富的执行统计和性能分析

## ✨ 功能特性

### 🎨 装饰器驱动
使用简洁的装饰器语法定义任务和工作流，代码清晰易读

### 🖥️ Web 管理界面
- 工作流列表和详情查看
- 实时执行状态监控
- 执行历史和日志查看
- 定时触发器管理
- 统计图表和性能分析

### ⚡ 高性能调度
- 基于 APScheduler 的强大调度引擎
- 支持 Cron 表达式定时执行
- 并发执行控制
- 任务队列管理

### 📊 数据持久化
- SQLite 数据库存储
- 完整的执行历史记录
- 结构化日志管理
- 数据导出功能

## 🛠️ 安装方法

### 从 GitHub 安装（推荐）

```bash
# 直接从 GitHub 安装最新版本
pip install git+https://github.com/liangwp001/flowy.git

# 或者安装特定版本
pip install git+https://github.com/liangwp001/flowy.git@v0.1.0
```

### 从源码安装

```bash
# 克隆仓库
git clone https://github.com/liangwp001/flowy.git
cd flowy

# 安装依赖并安装
pip install -e .
```

### 依赖要求

- Python 3.10+
- Flask 3.0+
- SQLAlchemy 2.0+
- APScheduler 3.10+

## 🚀 快速开始

### 1. 创建你的第一个工作流

```python
from flowy import flow, task, run, get_flow_logger

# 定义任务
@task(name="数据验证", desc="验证输入数据的有效性")
def validate_data(data: dict) -> dict:
    logger = get_flow_logger()
    logger.info(f"开始验证数据: {data}")
    
    if not data.get('name') or not data.get('age'):
        raise ValueError("缺少必要字段")
    
    logger.info("数据验证通过")
    return data

@task(name="数据处理", desc="处理和转换数据")
def process_data(data: dict) -> dict:
    logger = get_flow_logger()
    logger.info("开始处理数据")
    
    result = {
        'name': data['name'].upper(),
        'age': data['age'],
        'is_adult': data['age'] >= 18
    }
    
    logger.info(f"数据处理完成: {result}")
    return result

# 定义工作流
@flow(flow_id="user_data_flow", name="用户数据处理流程", desc="验证和处理用户数据")
def user_data_flow(name: str, age: int) -> dict:
    # 步骤1: 验证数据
    input_data = {'name': name, 'age': age}
    validated_data = validate_data(input_data)
    
    # 步骤2: 处理数据
    processed_data = process_data(validated_data)
    
    return processed_data

if __name__ == '__main__':
    # 执行工作流
    # result = user_data_flow(name="张三", age=25)
    # print(f"处理结果: {result}")
    
    # 启动 Web 管理界面
    run(host='127.0.0.1', port=5000, debug=True)
```

### 2. 启动 Web 界面

运行上述代码后，访问 `http://127.0.0.1:5000` 即可看到 Flowy 的管理界面。

### 3. 添加定时调度

通过 Web 界面或 API 为工作流添加定时触发器：

```python
# 通过 API 添加每日执行的触发器
import requests

trigger_data = {
    "name": "每日数据处理",
    "description": "每天凌晨2点执行用户数据处理",
    "cron_expression": "0 2 * * *",  # 每天凌晨2点
    "trigger_params": {
        "name": "系统用户",
        "age": 30
    }
}

response = requests.post(
    "http://127.0.0.1:5000/api/triggers",
    json=trigger_data
)
```

### 4. 监控和管理

在 Web 界面中你可以：
- 📊 查看工作流执行统计和趋势图表
- 📝 实时查看执行日志和错误信息
- ⏰ 管理定时触发器的启用/禁用状态
- 🔄 手动触发工作流执行并传入参数
- 📈 分析工作流性能和资源使用情况

## 🎯 使用场景

### 📊 数据处理管道
构建强大的 ETL 数据处理流水线：

```python
from flowy import flow, task, get_flow_logger
import pandas as pd

@task(name="数据提取", desc="从多个数据源提取数据")
def extract_data(sources: list) -> dict:
    logger = get_flow_logger()
    extracted_data = {}
    
    for source in sources:
        logger.info(f"正在提取数据源: {source['name']}")
        # 模拟数据提取
        data = pd.read_csv(source['path'])
        extracted_data[source['name']] = data
        logger.info(f"提取完成，记录数: {len(data)}")
    
    return extracted_data

@task(name="数据清洗", desc="清洗和验证数据质量")
def clean_data(raw_data: dict) -> dict:
    logger = get_flow_logger()
    cleaned_data = {}
    
    for source_name, data in raw_data.items():
        logger.info(f"正在清洗数据源: {source_name}")
        # 数据清洗逻辑
        cleaned = data.dropna().drop_duplicates()
        cleaned_data[source_name] = cleaned
        logger.info(f"清洗完成，有效记录: {len(cleaned)}")
    
    return cleaned_data

@task(name="数据加载", desc="将处理后的数据加载到目标系统")
def load_data(processed_data: dict, target_db: str) -> dict:
    logger = get_flow_logger()
    results = {}
    
    for source_name, data in processed_data.items():
        logger.info(f"正在加载数据: {source_name} -> {target_db}")
        # 模拟数据加载
        # data.to_sql(source_name, target_db, if_exists='replace')
        results[source_name] = len(data)
        logger.info(f"加载完成，记录数: {len(data)}")
    
    return results

@flow(flow_id="etl_pipeline", name="ETL数据管道", desc="完整的数据提取、转换、加载流程")
def etl_pipeline(config: dict) -> dict:
    """ETL数据处理管道
    
    Args:
        config: 包含数据源和目标配置的字典
    
    Returns:
        处理结果统计
    """
    logger = get_flow_logger()
    logger.info("开始执行ETL数据管道")
    
    # 步骤1: 提取数据
    raw_data = extract_data(config['sources'])
    
    # 步骤2: 清洗数据
    cleaned_data = clean_data(raw_data)
    
    # 步骤3: 加载数据
    load_results = load_data(cleaned_data, config['target_db'])
    
    # 生成执行报告
    total_records = sum(load_results.values())
    logger.info(f"ETL管道执行完成，总处理记录数: {total_records}")
    
    return {
        "total_records": total_records,
        "sources_processed": len(load_results),
        "details": load_results
    }
```

### 🔄 定时报表生成
自动化的报表生成和分发系统：

```python
@task(name="数据收集", desc="收集各系统的业务指标")
def collect_metrics(date_range: dict) -> dict:
    logger = get_flow_logger()
    logger.info(f"收集 {date_range['start']} 到 {date_range['end']} 的数据")
    
    # 模拟从各个系统收集数据
    metrics = {
        'sales': {'revenue': 150000, 'orders': 1200},
        'users': {'new_users': 450, 'active_users': 8900},
        'system': {'uptime': 99.9, 'response_time': 120}
    }
    
    return metrics

@task(name="报表生成", desc="生成多格式业务报表")
def generate_report(metrics: dict, template: str) -> dict:
    logger = get_flow_logger()
    logger.info(f"使用模板 {template} 生成报表")
    
    # 生成报表文件
    report_data = {
        'file_path': f'/reports/daily_report_{datetime.now().strftime("%Y%m%d")}.pdf',
        'summary': metrics,
        'charts': ['revenue_trend', 'user_growth', 'system_health']
    }
    
    logger.info("报表生成完成")
    return report_data

@task(name="报表分发", desc="将报表发送给相关人员")
def distribute_report(report: dict, recipients: list) -> dict:
    logger = get_flow_logger()
    
    sent_count = 0
    for recipient in recipients:
        logger.info(f"发送报表给: {recipient['email']}")
        # 模拟邮件发送
        sent_count += 1
    
    return {"sent_count": sent_count, "report_path": report['file_path']}

@flow(flow_id="daily_report", name="每日业务报表", desc="自动生成和分发每日业务报表")
def daily_report_flow(report_date: str = None) -> dict:
    from datetime import datetime, timedelta
    
    if not report_date:
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # 收集指标数据
    date_range = {'start': report_date, 'end': report_date}
    metrics = collect_metrics(date_range)
    
    # 生成报表
    report = generate_report(metrics, 'daily_business_template')
    
    # 分发报表
    recipients = [
        {'email': 'ceo@company.com', 'name': 'CEO'},
        {'email': 'cto@company.com', 'name': 'CTO'},
        {'email': 'sales@company.com', 'name': 'Sales Team'}
    ]
    distribution_result = distribute_report(report, recipients)
    
    return {
        'report_date': report_date,
        'metrics_summary': metrics,
        'distribution': distribution_result
    }
```


## 🖥️ Web界面

Flowy 提供了现代化、直观的 Web 管理界面，让工作流管理变得轻松愉快。

### 📋 工作流管理
![工作流列表](pics/flow-list.png)

- **流程列表**: 查看所有已定义的工作流，支持搜索和筛选
- **执行统计**: 成功率、平均执行时间等关键指标
- **实时监控**: 当前运行状态和进度跟踪
- **一键执行**: 直接在界面中触发工作流执行

### 📊 执行历史
![执行历史](pics/execution-history.png)

- **详细日志**: 每个任务的执行日志和输出
- **错误追踪**: 完整的错误堆栈和调试信息
- **性能分析**: 执行时间分析和瓶颈识别
- **数据可视化**: 图表展示执行趋势

### 📈 Flow 详情与统计
![Flow详情](pics/flow-detail.png)

- **执行统计**: 成功率、平均执行时间等关键指标
- **性能图表**: 执行趋势、时长分布、24小时分析
- **触发器管理**: 定时任务的创建和管理
- **实时监控**: 当前运行状态和历史记录

### ⏰ 定时触发器
![定时触发器](pics/triggers-management.png)

- **Cron 触发器**: 灵活的定时执行配置
- **触发器管理**: 启用/禁用、编辑调度规则
- **执行计划**: 查看即将执行的任务
- **参数配置**: 为定时任务配置执行参数

### 🔍 执行详情
![执行详情](pics/execution-detail.png)

- **详细信息**: 完整的执行过程和结果
- **输入输出**: JSON 格式的数据展示
- **任务追踪**: 每个子任务的执行状态
- **错误诊断**: 完整的错误堆栈和调试信息

## 📚 API文档

### 装饰器

#### `@task(name=None, desc=None)`
定义一个可重用的任务单元

**参数:**
- `name` (str, 可选): 任务显示名称
- `desc` (str, 可选): 任务描述

#### `@flow(flow_id, name=None, desc=None)`
定义一个工作流

**参数:**
- `flow_id` (str, 必需): 工作流唯一标识符
- `name` (str, 可选): 工作流显示名称
- `desc` (str, 可选): 工作流描述

### 核心函数

#### `run(host='127.0.0.1', port=5000, debug=False)`
启动 Web 管理界面

**参数:**
- `host` (str): 服务器地址
- `port` (int): 端口号
- `debug` (bool): 调试模式

#### `get_flow_logger()`
获取当前工作流的日志记录器

**返回:** `logging.Logger` 对象

#### `configure(data_dir=None)`
配置 Flowy 数据目录

**参数:**
- `data_dir` (str, 可选): 数据存储目录路径

### REST API

Flowy 提供完整的 REST API 用于集成和自动化：

```bash
# 获取工作流列表
GET /api/flows

# 触发工作流执行
POST /api/flows/{flow_id}/trigger

# 获取执行历史
GET /api/flows/{flow_id}/history

# 管理定时触发器
GET/POST/PUT/DELETE /api/triggers
```

## ⚙️ 高级功能

### 🔄 错误处理和重试
```python
@task(name="网络请求", desc="带重试机制的网络请求")
def fetch_data_with_retry(url: str, max_retries: int = 3) -> dict:
    logger = get_flow_logger()
    
    for attempt in range(max_retries):
        try:
            logger.info(f"尝试请求 {url} (第 {attempt + 1} 次)")
            # 模拟网络请求
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"请求失败: {e}")
            if attempt == max_retries - 1:
                logger.error("达到最大重试次数，请求失败")
                raise
            time.sleep(2 ** attempt)  # 指数退避
```

### 📊 条件分支和并行执行
```python
@flow(flow_id="conditional_flow", name="条件分支流程")
def conditional_processing_flow(data_type: str, data: dict) -> dict:
    logger = get_flow_logger()
    
    # 数据验证（总是执行）
    validated_data = validate_input(data)
    
    # 根据数据类型选择不同的处理路径
    if data_type == "user_data":
        result = process_user_data(validated_data)
    elif data_type == "order_data":
        result = process_order_data(validated_data)
    elif data_type == "product_data":
        result = process_product_data(validated_data)
    else:
        raise ValueError(f"不支持的数据类型: {data_type}")
    
    # 最终处理（总是执行）
    final_result = finalize_processing(result)
    return final_result
```

### 🔐 参数加密和安全
```python
from flowy import flow, task, get_flow_logger
import os
from cryptography.fernet import Fernet

@task(name="安全数据处理", desc="处理敏感数据")
def process_sensitive_data(encrypted_config: str) -> dict:
    logger = get_flow_logger()
    
    # 从环境变量获取解密密钥
    key = os.environ.get('FLOWY_ENCRYPTION_KEY')
    if not key:
        raise ValueError("未找到加密密钥")
    
    # 解密配置
    fernet = Fernet(key.encode())
    config = json.loads(fernet.decrypt(encrypted_config.encode()))
    
    logger.info("开始处理敏感数据（已脱敏日志）")
    # 处理敏感数据...
    
    return {"status": "success", "records_processed": 100}
```

## ⚙️ 配置

### 基本配置
```python
from flowy import configure

# 自定义数据目录
configure(data_dir='/path/to/your/data')
```

### 环境变量
```bash
# 数据目录
export FLOWY_DATA_DIR=/path/to/data

# 日志级别
export FLOWY_LOG_LEVEL=INFO

# Web界面端口
export FLOWY_WEB_PORT=5000

# 数据库连接（可选，默认使用SQLite）
export FLOWY_DATABASE_URL=postgresql://user:pass@localhost/flowy

# 加密密钥（用于敏感数据）
export FLOWY_ENCRYPTION_KEY=your-secret-key
```

### 配置文件
创建 `flowy.yaml` 配置文件：

```yaml
# Flowy 配置文件
database:
  url: "sqlite:///data/flowy.db"
  echo: false

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

web:
  host: "127.0.0.1"
  port: 5000
  debug: false

scheduler:
  max_workers: 10
  coalesce: true
  misfire_grace_time: 300

security:
  secret_key: "your-secret-key-here"
  session_timeout: 3600
```

## 🚀 性能优化

### 📈 最佳实践

#### 1. 任务设计原则
```python
# ✅ 好的做法：任务职责单一，易于测试和重用
@task(name="数据验证")
def validate_user_data(data: dict) -> dict:
    # 只负责数据验证
    pass

@task(name="数据转换")
def transform_user_data(data: dict) -> dict:
    # 只负责数据转换
    pass

# ❌ 避免：一个任务做太多事情
@task(name="处理所有数据")
def process_everything(data: dict) -> dict:
    # 验证、转换、保存都在一个任务中
    pass
```

#### 2. 错误处理策略
```python
@task(name="健壮的任务")
def robust_task(data: dict) -> dict:
    logger = get_flow_logger()
    
    try:
        # 业务逻辑
        result = process_data(data)
        return result
    except ValidationError as e:
        logger.error(f"数据验证失败: {e}")
        raise  # 重新抛出，让工作流失败
    except NetworkError as e:
        logger.warning(f"网络错误，稍后重试: {e}")
        time.sleep(5)
        return process_data(data)  # 简单重试
    except Exception as e:
        logger.error(f"未预期的错误: {e}")
        # 记录详细信息用于调试
        logger.error(traceback.format_exc())
        raise
```

#### 3. 资源管理
```python
@task(name="资源管理示例")
def resource_managed_task(file_path: str) -> dict:
    logger = get_flow_logger()
    
    # 使用上下文管理器确保资源正确释放
    with open(file_path, 'r') as file:
        data = file.read()
    
    # 对于数据库连接
    with get_database_connection() as conn:
        result = conn.execute("SELECT * FROM users")
    
    return {"processed": True}
```

### 📊 监控和调试

#### 性能监控
```python
import time
from functools import wraps

def monitor_performance(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_flow_logger()
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"任务 {func.__name__} 执行时间: {execution_time:.2f}秒")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"任务 {func.__name__} 执行失败，耗时: {execution_time:.2f}秒")
            raise
    
    return wrapper

@task(name="性能监控任务")
@monitor_performance
def monitored_task(data: dict) -> dict:
    # 任务逻辑
    pass
```

## 🔧 故障排除

### 常见问题

#### 1. 工作流执行失败
```bash
# 查看详细日志
tail -f data/log/flow-history-{history_id}.log

# 检查数据库状态
sqlite3 data/flowy.db ".tables"
```

#### 2. Web界面无法访问
```python
# 检查端口是否被占用
netstat -an | grep :5000

# 使用不同端口启动
run(host='127.0.0.1', port=8080)
```

#### 3. 定时任务不执行
```python
# 检查触发器状态
from flowy.web.services.trigger_service import TriggerService
triggers = TriggerService.get_triggers_by_flow('your_flow_id')
for trigger in triggers:
    print(f"触发器 {trigger.name}: 启用={trigger.enabled}")
```

## 🤝 贡献指南

我们欢迎所有形式的贡献！

### 开发环境设置
```bash
# 1. 克隆仓库
git clone https://github.com/liangwp001/flowy.git
cd flowy

# 2. 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows

# 3. 安装开发依赖
pip install -e ".[dev]"

# 4. 运行测试
pytest tests/

# 5. 代码格式化
black flowy/
isort flowy/
```

### 贡献流程
1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 编写代码和测试
4. 确保所有测试通过 (`pytest`)
5. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
6. 推送到分支 (`git push origin feature/AmazingFeature`)
7. 开启 Pull Request

### 代码规范
- 遵循 PEP 8 代码风格
- 为新功能添加测试用例
- 更新相关文档
- 提交信息使用英文，格式清晰

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

- [Flask](https://flask.palletsprojects.com/) - Web 框架
- [APScheduler](https://apscheduler.readthedocs.io/) - 任务调度
- [SQLAlchemy](https://www.sqlalchemy.org/) - 数据库 ORM
- [Chart.js](https://www.chartjs.org/) - 图表库

---

<div align="center">

**如果 Flowy 对你有帮助，请给我们一个 ⭐️**

[报告问题](https://github.com/liangwp001/flowy/issues) • [功能请求](https://github.com/liangwp001/flowy/issues) • [讨论](https://github.com/liangwp001/flowy/discussions)

</div>
