#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy备注模块 - 用于在工作流执行过程中添加备注信息"""

from typing import Literal
from flowy.core.context import get_flow_history_id
from flowy.core.db import get_session, add_flow_remark

RemarkLevel = Literal['info', 'warning', 'error']


def add_remark(level: RemarkLevel, message: str) -> bool:
    """为当前Flow执行历史添加备注

    Args:
        level: 备注级别 (info/warning/error)
        message: 备注消息

    Returns:
        是否添加成功

    Example:
        >>> from flowy import remark
        >>>
        >>> @task(name="处理任务")
        >>> def process_task(data):
        >>>     # 添加 info 级别备注
        >>>     remark.add_remark('info', '开始处理数据')
        >>>
        >>>     # 添加 warning 级别备注
        >>>     if len(data) > 1000:
        >>>         remark.add_remark('warning', f'数据量较大: {len(data)} 条')
        >>>
        >>>     # 添加 error 级别备注
        >>>     try:
        >>>         result = process(data)
        >>>     except Exception as e:
        >>>         remark.add_remark('error', f'处理失败: {e}')
        >>>         raise
        >>>
        >>>     return result
    """
    flow_history_id = get_flow_history_id()
    if not flow_history_id:
        return False

    session = get_session()
    try:
        return add_flow_remark(session, flow_history_id, level, message)
    finally:
        session.close()


def add_info(message: str) -> bool:
    """添加 info 级别备注

    Args:
        message: 备注消息

    Returns:
        是否添加成功
    """
    return add_remark('info', message)


def add_warning(message: str) -> bool:
    """添加 warning 级别备注

    Args:
        message: 备注消息

    Returns:
        是否添加成功
    """
    return add_remark('warning', message)


def add_error(message: str) -> bool:
    """添加 error 级别备注

    Args:
        message: 备注消息

    Returns:
        是否添加成功
    """
    return add_remark('error', message)
