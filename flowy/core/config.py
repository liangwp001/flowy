#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy配置模块"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FlowyConfig:
    """Flowy配置类"""
    # 数据目录，默认为当前工作目录下的 data 文件夹
    data_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), 'data'))

    @property
    def database_dir(self) -> str:
        """数据库目录"""
        return self.data_dir

    @property
    def log_dir(self) -> str:
        """日志目录"""
        return os.path.join(self.data_dir, 'log')

    @property
    def database_file(self) -> str:
        """数据库文件路径"""
        return os.path.join(self.database_dir, 'flowy.db')

    @property
    def database_url(self) -> str:
        """数据库连接URL"""
        return f'sqlite:///{self.database_file}'


# 全局配置实例
_config: Optional[FlowyConfig] = None


def get_config() -> FlowyConfig:
    """获取全局配置"""
    global _config
    if _config is None:
        _config = FlowyConfig()
    return _config


def configure(data_dir: Optional[str] = None) -> FlowyConfig:
    """配置Flowy

    Args:
        data_dir: 数据目录路径，默认为当前工作目录下的 data 文件夹

    Returns:
        FlowyConfig: 配置对象
    """
    global _config
    if data_dir is not None:
        _config = FlowyConfig(data_dir=data_dir)
    else:
        _config = FlowyConfig()
    return _config
