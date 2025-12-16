#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy任务装饰器"""
import inspect
import traceback
from datetime import datetime
from functools import wraps

from flowy.core.json_utils import json

from flowy.core.context import get_flow_history_id
from flowy.core.db import get_session, create_task_history, update_task_history
from flowy.core.logger import get_flow_logger


def task(name=None, desc=None):
    """Task装饰器，用于定义任务

    Args:
        name: 任务名称，默认使用函数名
        desc: 任务描述

    Returns:
        装饰器函数
    """
    def decorate(func):
        task_name = name or func.__name__

        @wraps(func)
        def wrapper(*args, **kwargs):
            flow_history_id = get_flow_history_id()
            logger = get_flow_logger()

            bound = inspect.signature(func).bind_partial(*args, **kwargs)
            bound.apply_defaults()  # 补上默认值
            arg_dict = dict(bound.arguments)

            input_data = arg_dict

            task_history_id = None
            if flow_history_id:
                session = get_session()
                try:
                    # 安全序列化输入数据
                    try:
                        input_json = json.dumps(input_data)
                    except Exception as serialize_error:
                        logger.warning(f"任务输入数据序列化失败: {serialize_error}")
                        input_json = json.dumps({
                            "error": "输入数据无法序列化",
                            "type": str(type(input_data))
                        })

                    task_history = create_task_history(
                        session=session,
                        flow_history_id=flow_history_id,
                        name=task_name,
                        created_at=datetime.now(),
                        start_time=datetime.now(),
                        input_data=input_json,
                        status='running'
                    )
                    session.commit()
                    task_history_id = task_history.id
                except Exception as e:
                    session.rollback()
                    logger.error(f"创建任务历史失败: {e}")
                finally:
                    session.close()

            result = None
            error_msg = None

            try:
                logger.info(f'{"-" * 50} task {task_name} start {"-" * 50}')
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(traceback.format_exc())
                raise
            finally:
                if task_history_id:
                    end_time = datetime.now()
                    session = get_session()
                    try:
                        output_data_json = None

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
                        except Exception as serialize_error:
                            logger.warning(f"任务输出数据序列化失败: {serialize_error}")
                            # 创建错误信息对象
                            error_output = {
                                'error': '输出数据无法序列化',
                                'serialize_error': str(serialize_error),
                                'result_type': str(type(result)),
                                'result_string': str(result)[:1000] if result is not None else None
                            }
                            try:
                                output_data_json = json.dumps(error_output)
                            except Exception as final_error:
                                logger.error(f"任务输出数据完全无法序列化: {final_error}")
                                output_data_json = json.dumps({
                                    'error': '输出数据完全无法序列化',
                                    'final_error': str(final_error)
                                })
                            if not error_msg:
                                status = 'completed_with_serialization_error'

                        # 更新任务历史
                        try:
                            update_task_history(
                                session=session,
                                task_id=task_history_id,
                                end_time=end_time,
                                output_data=output_data_json,
                                status=status
                            )
                            session.commit()
                        except Exception as db_error:
                            session.rollback()
                            logger.error(f"更新任务历史失败: {db_error}")
                    except Exception as e:
                        session.rollback()
                        logger.error(f"更新任务历史失败: {e}")
                    finally:
                        session.close()
                logger.info(f'{"-" * 50} task {task_name} end {"-" * 50}')

        return wrapper

    return decorate
