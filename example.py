"""
Flowy 多步骤工作流示例
演示如何使用框架构建一个完整的数据处理工作流
"""

from flowy import flow, task, run, get_flow_logger, set_progress
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

    # 更新进度：开始转换
    set_progress(10, "准备转换数据...")

    # 模拟数据处理步骤
    time.sleep(0.5)
    set_progress(30, "转换名称字段...")

    time.sleep(0.3)
    name = data['name'].upper()
    set_progress(50, "计算出生年份...")

    time.sleep(0.3)
    birth_year = 2025 - data['age']
    set_progress(70, "判断是否成年...")

    time.sleep(0.2)
    is_adult = data['age'] >= 18
    set_progress(90, "整合转换结果...")

    time.sleep(0.1)
    transformed = {
        'name': name,
        'age': data['age'],
        'birth_year': birth_year,
        'is_adult': is_adult
    }

    set_progress(100, "数据转换完成")
    logger.info(f"数据转换完成: {transformed}")
    return transformed


@task(name="数据丰富", desc="添加额外的计算字段")
def enrich_data(data: dict) -> dict:
    """丰富数据，添加额外信息"""
    logger = get_flow_logger()
    logger.info(f"开始丰富数据")

    # 更新进度
    set_progress(20, "计算代际信息...")
    time.sleep(0.3)
    generation = get_generation(data['birth_year'])

    set_progress(50, "判断年龄段...")
    time.sleep(0.3)
    age_group = get_age_group(data['age'])

    set_progress(80, "添加处理时间戳...")
    time.sleep(0.2)
    processed_at = time.strftime('%Y-%m-%d %H:%M:%S')

    set_progress(100, "数据丰富完成")
    enriched = {
        **data,
        'generation': generation,
        'age_group': age_group,
        'processed_at': processed_at
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


@task(name="批量数据处理", desc="批量处理多个数据项")
def batch_process_data(items: list) -> list:
    """批量处理数据列表，展示进度更新

    这个任务展示了如何在循环中更新进度。
    """
    logger = get_flow_logger()
    logger.info(f"开始批量处理 {len(items)} 个数据项")

    total = len(items)
    results = []

    for i, item in enumerate(items):
        # 计算当前进度
        progress = int((i + 1) / total * 100)
        set_progress(progress, f"正在处理第 {i + 1}/{total} 项: {item.get('name', 'Unknown')}")

        # 模拟处理单个项目
        time.sleep(0.8)

        # 处理项目
        processed = {
            **item,
            'birth_year': 2025 - item['age'],
            'is_adult': item['age'] >= 18,
            'processed_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        results.append(processed)

    set_progress(100, f"批量处理完成，共处理 {total} 项")
    logger.info(f"批量处理完成: {len(results)} 个项目")
    return results


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

    # 步骤2: 转换数据（带进度更新）
    transformed_data = transform_data(validated_data)

    # 步骤3: 丰富数据（带进度更新）
    enriched_data = enrich_data(transformed_data)

    # 步骤4: 验证输出
    validated_output = validate_output(enriched_data)

    # 步骤5: 保存数据
    final_result = save_data(validated_output)

    logger.info(f"工作流执行完成")
    return final_result


@flow(flow_id="batch_processing_flow", name="批量数据处理工作流", desc="批量处理多个数据项的工作流")
def batch_processing_flow(items: list) -> dict:
    """
    批量数据处理工作流

    这个工作流展示了如何在处理列表时更新进度。

    Args:
        items: 要处理的数据列表，每个元素是包含 name 和 age 的字典

    Returns:
        处理结果摘要
    """
    logger = get_flow_logger()
    logger.info(f"批量处理工作流开始，共 {len(items)} 个项目")

    # 批量处理数据（带进度更新）
    processed_items = batch_process_data(items)

    result = {
        'total_processed': len(processed_items),
        'items': processed_items,
        'completed_at': time.strftime('%Y-%m-%d %H:%M:%S')
    }

    logger.info(f"批量处理工作流完成")
    return result


# ============ 主程序 ============

if __name__ == '__main__':
    run(host='127.0.0.1', port=5000, debug=True)
