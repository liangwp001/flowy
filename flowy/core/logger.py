#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy日志模块"""

import logging
import os

from flowy.core.config import get_config
from flowy.core.context import get_flow_history_id


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

    return logger
