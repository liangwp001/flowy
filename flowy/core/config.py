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

    # 历史数据清理配置
    enable_history_cleanup: bool = False  # 是否启用历史数据自动清理，默认关闭
    history_retention_days: int = 60  # 历史数据保留天数，默认60天

    # 调度器配置
    scheduler_max_workers: int = 10  # 调度器线程池最大工作线程数
    scheduler_timezone: str = 'Asia/Shanghai'  # 调度器时区

    # 站点配置
    site_name: str = 'Flowy'  # 自定义站点名称

    # 认证配置
    auth_enabled: bool = False  # 是否启用认证，默认关闭
    auth_username: Optional[str] = None  # 认证用户名
    auth_password: Optional[str] = None  # 认证密码

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
        """主数据库文件路径"""
        return os.path.join(self.database_dir, 'flowy.db')

    @property
    def history_database_file(self) -> str:
        """历史数据库文件路径"""
        return os.path.join(self.database_dir, 'flowy_history.db')

    @property
    def database_url(self) -> str:
        """主数据库连接URL"""
        return f'sqlite:///{self.database_file}'

    @property
    def history_database_url(self) -> str:
        """历史数据库连接URL"""
        return f'sqlite:///{self.history_database_file}'


# 全局配置实例
_config: Optional[FlowyConfig] = None


def get_config() -> FlowyConfig:
    """获取全局配置"""
    global _config
    if _config is None:
        _config = FlowyConfig()
    return _config


def configure(
    data_dir: Optional[str] = None,
    site_name: Optional[str] = None,
    auth_enabled: bool = False,
    auth_username: Optional[str] = None,
    auth_password: Optional[str] = None,
    **kwargs
) -> FlowyConfig:
    """配置Flowy

    Args:
        data_dir: 数据目录路径，默认为当前工作目录下的 data 文件夹
        site_name: 自定义站点名称，默认为 'Flowy'
        auth_enabled: 是否启用认证，默认关闭
        auth_username: 认证用户名
        auth_password: 认证密码
        **kwargs: 其他配置参数

    Returns:
        FlowyConfig: 配置对象
    """
    global _config
    config_kwargs = {}

    if data_dir is not None:
        config_kwargs['data_dir'] = data_dir
    if site_name is not None:
        config_kwargs['site_name'] = site_name
    if auth_enabled:
        config_kwargs['auth_enabled'] = auth_enabled
    if auth_username is not None:
        config_kwargs['auth_username'] = auth_username
    if auth_password is not None:
        config_kwargs['auth_password'] = auth_password

    # 合并其他配置参数
    config_kwargs.update(kwargs)

    _config = FlowyConfig(**config_kwargs)
    return _config
