#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy工作流装饰器"""
import inspect
import traceback
from datetime import datetime
from functools import wraps

from flowy.core.json_utils import json

from flowy.core.context import flow_history_id_var
from flowy.core.db import init_database, get_session, create_flow_history, update_flow_history, register_flow, FlowHistory
from flowy.core.logger import get_flow_logger, cleanup_flow_logger

# 初始化数据库
init_database()

# 全局Flow注册表
_flow_registry = {}

def get_flow_registry():
    """获取Flow注册表"""
    return _flow_registry

def register_flow_func(flow_id, func):
    """注册Flow函数到注册表"""
    _flow_registry[flow_id] = func

def get_flow_func(flow_id):
    """从注册表获取Flow函数"""
    return _flow_registry.get(flow_id)

def execute_flow(flow_id, input_data=None, metadata=None):
    """执行指定的Flow"""
    func = get_flow_func(flow_id)
    if not func:
        return {
            "success": False,
            "error": f"Flow '{flow_id}' 未找到"
        }

    try:
        kwargs = input_data or {}
        # wrapper 函数会自动处理 _metadata，所以直接传递
        kwargs['_metadata'] = metadata or {}
        
        # 执行Flow（实际上是调用 wrapper）
        result = func(**kwargs)
        return {
            "success": True,
            "result": result
        }
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": f"{type(e).__name__}: {str(e)}",
            "traceback": traceback.format_exc()
        }


def flow(flow_id, name=None, desc=None):
    """Flow装饰器，用于定义工作流

    Args:
        flow_id: 工作流唯一标识
        name: 工作流名称，默认使用flow_id
        desc: 工作流描述

    Returns:
        装饰器函数
    """
    def decorate(func):


        # 注册到数据库
        session = get_session()
        register_flow(session=session, flow_id=flow_id, name=name or flow_id, desc=desc)
        session.commit()
        session.close()
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 提取并移除 _metadata，不传递给原始函数
            metadata = kwargs.pop('_metadata', None)

            # 绑定参数以获取完整的参数字典（用于记录）
            bound = inspect.signature(func).bind_partial(*args, **kwargs)
            bound.apply_defaults()  # 补上默认值
            arg_dict = dict(bound.arguments)

            input_data = arg_dict

            created_at = datetime.now()
            start_time = created_at

            session = get_session()
            flow_history_id = None
            logger = get_flow_logger()
            try:
                input_json = json.dumps(input_data)
            except Exception as serialize_error:
                logger.warning(f"输入数据序列化失败: {serialize_error}")
                input_json = json.dumps({"error": "输入数据无法序列化", "type": str(type(input_data))})

            metadata_json = "{}"
            # 检查metadata中是否已有flow_history_id
            if metadata and isinstance(metadata, dict):
                flow_history_id = metadata.get('flow_history_id')
                try:
                    metadata_json = json.dumps(metadata)
                except Exception as serialize_error:
                    logger.warning(f"元数据序列化失败: {serialize_error}")
                    metadata_json = json.dumps({"error": "元数据无法序列化", "type": str(type(metadata))})

            try:
                if flow_history_id:
                    # 如果已有历史记录ID，更新状态为running
                    history = session.query(FlowHistory).filter(FlowHistory.id == flow_history_id).first()
                    history.status = 'running'
                    history.start_time = start_time
                    history.input_data = input_json
                    history.flow_metadata = metadata_json
                    session.commit()
                else:
                    # 如果没有历史记录ID，创建新的
                    history = create_flow_history(
                        session=session,
                        created_at=created_at,
                        start_time=start_time,
                        input_data=input_json,
                        status='running',
                        flow_id=flow_id,
                    )
                    session.commit()
                    flow_history_id = history.id


            except Exception as e:
                session.rollback()
                logger.error(f"创建或更新flow历史失败: {e}")
            finally:
                session.close()

            token = None
            if flow_history_id is not None:
                token = flow_history_id_var.set(flow_history_id)

            result = None
            error_msg = None
            try:
                logger.info(f'{"-" * 50} flow {name} start {"-" * 50}')
                # 调用原始函数时不传递 _metadata
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(traceback.format_exc())
                raise
            finally:
                if token is not None:
                    flow_history_id_var.reset(token)
                if flow_history_id:
                    session = get_session()
                    try:
                        end_time = datetime.now()

                        if error_msg:
                            # 如果有执行错误，记录错误信息
                            output_data = {
                                'error': error_msg,
                                'traceback': traceback.format_exc()
                            }
                            status = 'failed'
                        else:
                            # 尝试序列化正常结果
                            output_data = {
                                'result': result,
                            }
                            status = 'completed'

                        # 最终序列化输出数据
                        try:
                            output_data_json = json.dumps(output_data)
                        except Exception as final_serialize_error:
                            logger.error(f"最终输出数据序列化也失败: {final_serialize_error}")
                            output_data_json = json.dumps({
                                'error': '输出数据完全无法序列化',
                                'final_error': str(final_serialize_error)
                            })

                        # 更新flow历史
                        try:
                            update_flow_history(
                                session=session,
                                history_id=flow_history_id,
                                end_time=end_time,
                                output_data=output_data_json,
                                status=status
                            )
                            session.commit()
                        except Exception as db_error:
                            session.rollback()
                            logger.error(f"更新flow历史失败: {db_error}")
                    except Exception as e:
                        session.rollback()
                        logger.error(f"更新flow历史失败: {e}")
                    finally:
                        session.close()
                logger.info(f'{"-" * 50} flow {name} end {"-" * 50}')

                # 清理 flow history logger，释放文件句柄和内存
                if flow_history_id:
                    cleanup_flow_logger(flow_history_id)

        register_flow_func(flow_id=flow_id, func=wrapper)
        return wrapper

    return decorate
