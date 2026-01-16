#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy配置模块"""

import os
from dataclasses import dataclass, field
from typing import Optional, List


# 有效的运行模式
VALID_MODES = ('standalone', 'master', 'worker')


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

    # 分布式配置
    mode: str = 'standalone'  # 运行模式: standalone / master / worker
    external_database_url: Optional[str] = None  # 外部数据库连接串 (PostgreSQL/MySQL)

    # 数据库连接池配置
    database_pool_size: int = 5  # 连接池大小
    database_max_overflow: int = 10  # 最大溢出连接数
    database_pool_timeout: int = 30  # 连接池超时（秒）

    # 任务调度配置
    task_claim_timeout: int = 300  # claimed 状态超时（秒），默认5分钟
    task_running_timeout: int = 86400  # running 状态超时（秒），默认24小时

    # 心跳配置
    worker_heartbeat_interval: int = 30  # 心跳间隔（秒）
    worker_heartbeat_timeout: int = 150  # 心跳超时（秒）

    # Worker 标签（仅 worker 模式使用）
    worker_tags: List[str] = field(default_factory=list)

    def validate(self) -> None:
        """验证配置有效性
        
        Raises:
            ValueError: 配置无效时抛出
        """
        # 验证运行模式
        if self.mode not in VALID_MODES:
            raise ValueError(
                f"Invalid mode: '{self.mode}'. Must be one of: {', '.join(VALID_MODES)}"
            )
        
        # 分布式模式验证
        if self.mode in ('master', 'worker'):
            # 必须配置外部数据库
            if not self.external_database_url:
                raise ValueError(
                    f"external_database_url is required for {self.mode} mode"
                )
            
            # 不支持 SQLite
            if self.external_database_url.startswith('sqlite'):
                raise ValueError(
                    "SQLite is not supported in distributed mode. "
                    "Please use PostgreSQL or MySQL."
                )
        
        # 验证数值配置
        if self.database_pool_size < 1:
            raise ValueError("database_pool_size must be at least 1")
        
        if self.database_max_overflow < 0:
            raise ValueError("database_max_overflow must be non-negative")
        
        if self.database_pool_timeout < 1:
            raise ValueError("database_pool_timeout must be at least 1")
        
        if self.task_claim_timeout < 1:
            raise ValueError("task_claim_timeout must be at least 1")
        
        if self.task_running_timeout < 1:
            raise ValueError("task_running_timeout must be at least 1")
        
        if self.worker_heartbeat_interval < 1:
            raise ValueError("worker_heartbeat_interval must be at least 1")
        
        if self.worker_heartbeat_timeout < self.worker_heartbeat_interval:
            raise ValueError(
                "worker_heartbeat_timeout must be greater than or equal to "
                "worker_heartbeat_interval"
            )

    @property
    def is_distributed(self) -> bool:
        """是否为分布式模式"""
        return self.mode in ('master', 'worker')

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
        """主数据库连接URL
        
        分布式模式下返回外部数据库URL，否则返回本地SQLite URL
        """
        if self.external_database_url:
            return self.external_database_url
        return f'sqlite:///{self.database_file}'

    @property
    def history_database_url(self) -> str:
        """历史数据库连接URL
        
        分布式模式下返回外部数据库URL，否则返回本地SQLite URL
        """
        if self.external_database_url:
            return self.external_database_url
        return f'sqlite:///{self.history_database_file}'


# 全局配置实例
_config: Optional[FlowyConfig] = None


def get_config() -> FlowyConfig:
    """获取全局配置"""
    global _config
    if _config is None:
        _config = FlowyConfig()
    return _config


def set_config(config: FlowyConfig) -> None:
    """设置全局配置
    
    Args:
        config: 配置对象
    """
    global _config
    _config = config


def configure(data_dir: Optional[str] = None, **kwargs) -> FlowyConfig:
    """配置Flowy

    Args:
        data_dir: 数据目录路径，默认为当前工作目录下的 data 文件夹
        **kwargs: 其他配置参数

    Returns:
        FlowyConfig: 配置对象
    """
    global _config
    config_kwargs = {}
    
    if data_dir is not None:
        config_kwargs['data_dir'] = data_dir
    
    # 合并其他配置参数
    config_kwargs.update(kwargs)
    
    _config = FlowyConfig(**config_kwargs)
    return _config


def configure_from_env() -> FlowyConfig:
    """从环境变量配置Flowy
    
    支持的环境变量:
        - FLOWY_MODE: 运行模式 (standalone/master/worker)
        - FLOWY_DATABASE_URL: 外部数据库连接URL
        - FLOWY_WORKER_TAGS: Worker标签，逗号分隔
        - FLOWY_DATA_DIR: 数据目录
        - FLOWY_POOL_SIZE: 数据库连接池大小
        - FLOWY_POOL_MAX_OVERFLOW: 连接池最大溢出
        - FLOWY_POOL_TIMEOUT: 连接池超时
        - FLOWY_CLAIM_TIMEOUT: 任务认领超时
        - FLOWY_RUNNING_TIMEOUT: 任务运行超时
        - FLOWY_HEARTBEAT_INTERVAL: 心跳间隔
        - FLOWY_HEARTBEAT_TIMEOUT: 心跳超时
    
    Returns:
        FlowyConfig: 配置对象
    """
    global _config
    
    config_kwargs = {}
    
    # 运行模式
    mode = os.environ.get('FLOWY_MODE')
    if mode:
        config_kwargs['mode'] = mode
    
    # 数据库URL
    database_url = os.environ.get('FLOWY_DATABASE_URL')
    if database_url:
        config_kwargs['external_database_url'] = database_url
    
    # Worker标签
    worker_tags = os.environ.get('FLOWY_WORKER_TAGS')
    if worker_tags:
        config_kwargs['worker_tags'] = [
            t.strip() for t in worker_tags.split(',') if t.strip()
        ]
    
    # 数据目录
    data_dir = os.environ.get('FLOWY_DATA_DIR')
    if data_dir:
        config_kwargs['data_dir'] = data_dir
    
    # 数据库连接池配置
    pool_size = os.environ.get('FLOWY_POOL_SIZE')
    if pool_size:
        config_kwargs['database_pool_size'] = int(pool_size)
    
    pool_max_overflow = os.environ.get('FLOWY_POOL_MAX_OVERFLOW')
    if pool_max_overflow:
        config_kwargs['database_max_overflow'] = int(pool_max_overflow)
    
    pool_timeout = os.environ.get('FLOWY_POOL_TIMEOUT')
    if pool_timeout:
        config_kwargs['database_pool_timeout'] = int(pool_timeout)
    
    # 任务超时配置
    claim_timeout = os.environ.get('FLOWY_CLAIM_TIMEOUT')
    if claim_timeout:
        config_kwargs['task_claim_timeout'] = int(claim_timeout)
    
    running_timeout = os.environ.get('FLOWY_RUNNING_TIMEOUT')
    if running_timeout:
        config_kwargs['task_running_timeout'] = int(running_timeout)
    
    # 心跳配置
    heartbeat_interval = os.environ.get('FLOWY_HEARTBEAT_INTERVAL')
    if heartbeat_interval:
        config_kwargs['worker_heartbeat_interval'] = int(heartbeat_interval)
    
    heartbeat_timeout = os.environ.get('FLOWY_HEARTBEAT_TIMEOUT')
    if heartbeat_timeout:
        config_kwargs['worker_heartbeat_timeout'] = int(heartbeat_timeout)
    
    _config = FlowyConfig(**config_kwargs)
    return _config
