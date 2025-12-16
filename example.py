"""
Flowy 多步骤工作流示例
演示如何使用框架构建一个完整的数据处理工作流
"""

from flowy import flow, task, run, get_flow_logger
import time


# ============ 定义任务 ============

@task(name="数据验证", desc="验证输入数据的有效性")
def validate_data(data: dict) -> dict:
    """验证输入数据"""
    logger = get_flow_logger()
    logger.info(f"开始验证数据: {data}")
    
    if not isinstance(data, dict):
        raise ValueError("输入数据必须是字典类型")
    
    if 'name' not in data or 'age' not in data:
        raise ValueError("数据必须包含 'name' 和 'age' 字段")
    
    if not isinstance(data['age'], int) or data['age'] < 0:
        raise ValueError("年龄必须是非负整数")
    
    logger.info("数据验证通过")
    return data


@task(name="数据转换", desc="将数据转换为标准格式")
def transform_data(data: dict) -> dict:
    """转换数据格式"""
    logger = get_flow_logger()
    logger.info(f"开始转换数据")
    
    # 模拟数据处理
    time.sleep(1)
    
    transformed = {
        'name': data['name'].upper(),
        'age': data['age'],
        'birth_year': 2025 - data['age'],
        'is_adult': data['age'] >= 18
    }
    
    logger.info(f"数据转换完成: {transformed}")
    return transformed


@task(name="数据丰富", desc="添加额外的计算字段")
def enrich_data(data: dict) -> dict:
    """丰富数据，添加额外信息"""
    logger = get_flow_logger()
    logger.info(f"开始丰富数据")
    
    # 模拟数据丰富过程
    time.sleep(1)
    
    enriched = {
        **data,
        'generation': get_generation(data['birth_year']),
        'age_group': get_age_group(data['age']),
        'processed_at': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    logger.info(f"数据丰富完成: {enriched}")
    return enriched


@task(name="数据验证", desc="验证处理后的数据")
def validate_output(data: dict) -> dict:
    """验证输出数据"""
    logger = get_flow_logger()
    logger.info(f"开始验证输出数据")
    
    required_fields = ['name', 'age', 'birth_year', 'is_adult', 'generation', 'age_group', 'processed_at']
    for field in required_fields:
        if field not in data:
            raise ValueError(f"输出数据缺少必需字段: {field}")
    
    logger.info("输出数据验证通过")
    return data


@task(name="数据保存", desc="保存处理结果")
def save_data(data: dict) -> dict:
    """保存数据（模拟）"""
    logger = get_flow_logger()
    logger.info(f"开始保存数据")
    
    # 模拟保存到数据库
    time.sleep(1)
    
    result = {
        **data,
        'saved': True,
        'record_id': f"REC_{int(time.time())}"
    }
    
    logger.info(f"数据保存成功，记录ID: {result['record_id']}")
    return result


# ============ 辅助函数 ============

def get_generation(birth_year: int) -> str:
    """根据出生年份获取代际"""
    if birth_year >= 2013:
        return "Gen Z"
    elif birth_year >= 1997:
        return "Millennial"
    elif birth_year >= 1981:
        return "Gen X"
    elif birth_year >= 1965:
        return "Baby Boomer"
    else:
        return "Silent Generation"


def get_age_group(age: int) -> str:
    """根据年龄获取年龄段"""
    if age < 13:
        return "Child"
    elif age < 18:
        return "Teenager"
    elif age < 65:
        return "Adult"
    else:
        return "Senior"


# ============ 定义工作流 ============

@flow(flow_id="data_processing_flow", name="数据处理工作流", desc="完整的多步骤数据处理工作流")
def data_processing_flow(name: str, age: int) -> dict:
    """
    多步骤数据处理工作流
    
    步骤：
    1. 验证输入数据
    2. 转换数据格式
    3. 丰富数据（添加计算字段）
    4. 验证输出数据
    5. 保存数据
    
    Args:
        name: 人员名称
        age: 人员年龄
    
    Returns:
        处理后的完整数据
    """
    logger = get_flow_logger()
    logger.info(f"工作流开始执行，输入参数: name={name}, age={age}")
    
    # 步骤1: 验证输入数据
    input_data = {'name': name, 'age': age}
    validated_data = validate_data(input_data)
    
    # 步骤2: 转换数据
    transformed_data = transform_data(validated_data)
    
    # 步骤3: 丰富数据
    enriched_data = enrich_data(transformed_data)
    
    # 步骤4: 验证输出
    validated_output = validate_output(enriched_data)
    
    # 步骤5: 保存数据
    final_result = save_data(validated_output)
    
    logger.info(f"工作流执行完成")
    return final_result


# ============ 主程序 ============

if __name__ == '__main__':
    import sys
    
    # 执行工作流示例
    print("=" * 60)
    print("Flowy 多步骤工作流示例")
    print("=" * 60)
    
    # 示例1: 成功的工作流执行
    print("\n【示例1】执行成功的工作流:")
    print("-" * 60)
    try:
        result = data_processing_flow(name="张三", age=28)
        print(f"\n✓ 工作流执行成功!")
        print(f"结果: {result}")
    except Exception as e:
        print(f"\n✗ 工作流执行失败: {e}")
    
    # 示例2: 数据验证失败
    print("\n\n【示例2】数据验证失败的工作流:")
    print("-" * 60)
    try:
        result = data_processing_flow(name="李四", age=-5)
        print(f"\n✓ 工作流执行成功!")
        print(f"结果: {result}")
    except Exception as e:
        print(f"\n✗ 工作流执行失败（预期行为）: {e}")
    
    # 示例3: 启动Web管理界面
    print("\n\n【示例3】启动Web管理界面:")
    print("-" * 60)
    print("访问 http://127.0.0.1:5000 查看工作流历史和执行详情")
    print("按 Ctrl+C 停止服务器\n")
    
    run(host='127.0.0.1', port=5000, debug=True)
