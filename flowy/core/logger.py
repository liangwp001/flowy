#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy日志模块"""

import logging
import os
import threading

from flowy.core.config import get_config
from flowy.core.context import get_flow_history_id

# 用于跟踪动态创建的 flow history logger，便于清理
_flow_history_loggers = set()
_logger_lock = threading.Lock()


def get_flow_logger():
    """获取当前Flow的logger"""
    flow_history_id = get_flow_history_id()
    if flow_history_id:
        return get_logger(f"flow-history-{flow_history_id}")
    else:
        return get_logger('flow')


def get_logger(name, console_output=False):
    """获取一个logger对象

    Args:
        name: logger名称，也用作日志文件名
        console_output: 是否同时输出到控制台，默认为False

    Returns:
        logging.Logger: logger对象
    """
    config = get_config()
    log_dir = config.log_dir
    os.makedirs(log_dir, exist_ok=True)

    logger_name = f"{name}_console" if console_output else name
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        log_file = os.path.join(log_dir, f'{name}.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

        # 跟踪 flow-history logger 以便后续清理
        if name.startswith('flow-history-'):
            with _logger_lock:
                _flow_history_loggers.add(logger_name)

    return logger


def cleanup_flow_logger(flow_history_id):
    """清理指定 flow history 的 logger，释放文件句柄和内存

    Args:
        flow_history_id: Flow 历史记录 ID
    """
    logger_names = [
        f"flow-history-{flow_history_id}",
        f"flow-history-{flow_history_id}_console"
    ]

    for logger_name in logger_names:
        try:
            logger = logging.getLogger(logger_name)

            # 关闭并移除所有 handler
            for handler in logger.handlers[:]:
                try:
                    handler.close()
                except Exception:
                    pass
                logger.removeHandler(handler)

            # 从跟踪集合中移除
            with _logger_lock:
                _flow_history_loggers.discard(logger_name)

            # 从 logging 模块的 manager 中移除 logger
            # 这是关键步骤，防止 logger 对象被全局缓存
            if logger_name in logging.Logger.manager.loggerDict:
                del logging.Logger.manager.loggerDict[logger_name]

        except Exception:
            pass  # 忽略清理过程中的错误


def cleanup_all_flow_loggers():
    """清理所有 flow history logger，用于应用关闭时调用"""
    with _logger_lock:
        logger_names = list(_flow_history_loggers)

    for logger_name in logger_names:
        try:
            logger = logging.getLogger(logger_name)
            for handler in logger.handlers[:]:
                try:
                    handler.close()
                except Exception:
                    pass
                logger.removeHandler(handler)

            if logger_name in logging.Logger.manager.loggerDict:
                del logging.Logger.manager.loggerDict[logger_name]
        except Exception:
            pass

    with _logger_lock:
        _flow_history_loggers.clear()


def get_active_flow_logger_count():
    """获取当前活跃的 flow history logger 数量，用于监控

    Returns:
        int: 活跃的 logger 数量
    """
    with _logger_lock:
        return len(_flow_history_loggers)
