#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy任务进度管理模块"""

from contextvars import ContextVar

from flowy.core.db import get_session, update_task_progress
from flowy.core.logger import get_flow_logger

# 当前任务历史ID的上下文变量
task_history_id_var: ContextVar[int] = ContextVar('task_history_id', default=None)


def set_task_history_id(task_id: int) -> None:
    """设置当前任务的ID到上下文

    Args:
        task_id: 任务历史记录ID
    """
    task_history_id_var.set(task_id)


def get_task_history_id() -> int | None:
    """获取当前上下文的任务ID

    Returns:
        当前任务的历史记录ID，如果未设置则返回 None
    """
    return task_history_id_var.get()


def set_progress(progress: int, message: str | None = None) -> bool:
    """设置当前任务的执行进度

    业务代码可以在任务执行过程中调用此函数来更新进度。

    Args:
        progress: 进度百分比 (0-100)
        message: 进度描述信息（可选）

    Returns:
        是否成功更新进度

    Example:
        @task(name="处理数据")
        def process_data(items):
            total = len(items)
            for i, item in enumerate(items):
                process_item(item)
                set_progress(
                    progress=int((i + 1) / total * 100),
                    message=f"正在处理第 {i+1}/{total} 项"
                )
            return {"processed": total}
    """
    # 验证进度值
    if not isinstance(progress, int) or not (0 <= progress <= 100):
        logger = get_flow_logger()
        logger.warning(f"无效的进度值: {progress}，必须在 0-100 之间")
        return False

    task_id = get_task_history_id()
    if task_id is None:
        # 不在任务上下文中，静默失败
        return False

    session = get_session()
    try:
        update_task_progress(
            session=session,
            task_id=task_id,
            progress=progress,
            progress_message=message
        )
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logger = get_flow_logger()
        logger.error(f"更新任务进度失败: {e}")
        return False
    finally:
        session.close()
