#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy数据库模块"""

import os
import zlib
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, create_engine, DateTime, Text, INTEGER, LargeBinary, TypeDecorator
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from flowy.core.config import get_config

Base = declarative_base()
HistoryBase = declarative_base()


class CompressedText(TypeDecorator):
    """压缩文本类型，自动压缩和解压缩"""
    impl = LargeBinary
    cache_ok = True

    def process_bind_param(self, value, dialect):
        """写入时压缩"""
        if value is None:
            return None
        if isinstance(value, str):
            value = value.encode('utf-8')
        # 使用最高压缩级别 9，优先考虑压缩率而非速度
        return zlib.compress(value, level=9)

    def process_result_value(self, value, dialect):
        """读取时解压"""
        if value is None:
            return None
        return zlib.decompress(value).decode('utf-8')


class Trigger(Base):
    __tablename__ = 'trigger'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    flow_id = Column(String(64))
    name = Column(String(64))
    description = Column(String(256))
    cron_expression = Column(String(128))
    trigger_params = Column(Text)
    max_instances = Column(INTEGER, default=1)
    enabled = Column(INTEGER, default=1)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class Flow(Base):
    __tablename__ = 'flow'
    id = Column(String(64), primary_key=True)
    name = Column(String(64))
    description = Column(String(64))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class FlowHistory(HistoryBase):
    __tablename__ = 'flow_history'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    flow_metadata = Column(Text, default="")
    flow_id = Column(String(64))
    created_at = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    input_data = Column(CompressedText)
    output_data = Column(CompressedText)
    status = Column(String(64))


class TaskHistory(HistoryBase):
    __tablename__ = 'task_history'
    id = Column(INTEGER, primary_key=True)
    flow_history_id = Column(INTEGER)
    name = Column(String(64))
    created_at = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    input_data = Column(CompressedText)
    output_data = Column(CompressedText)
    status = Column(String(64))
    progress = Column(INTEGER)  # 进度百分比 (0-100)，NULL 表示未设置
    progress_message = Column(String(256))  # 进度描述信息
    progress_updated_at = Column(DateTime)  # 进度最后更新时间


# 延迟初始化的数据库引擎和会话
_engine = None
_history_engine = None
_DBSession = None


def _get_engine():
    """获取主数据库引擎（延迟初始化）"""
    global _engine
    if _engine is None:
        config = get_config()
        os.makedirs(config.database_dir, exist_ok=True)
        _engine = create_engine(
            config.database_url,
            echo=False,
            pool_pre_ping=True,
            connect_args={
                'check_same_thread': False,
                'timeout': 20
            }
        )
    return _engine


def _get_history_engine():
    """获取历史数据库引擎（延迟初始化）"""
    global _history_engine
    if _history_engine is None:
        config = get_config()
        os.makedirs(config.database_dir, exist_ok=True)
        _history_engine = create_engine(
            config.history_database_url,
            echo=False,
            pool_pre_ping=True,
            connect_args={
                'check_same_thread': False,
                'timeout': 20
            }
        )
    return _history_engine


def _get_session_maker():
    """获取会话工厂（延迟初始化）"""
    global _DBSession
    if _DBSession is None:
        # 创建绑定到两个数据库的会话
        _DBSession = sessionmaker()
        _DBSession.configure(binds={
            Flow: _get_engine(),
            Trigger: _get_engine(),
            FlowHistory: _get_history_engine(),
            TaskHistory: _get_history_engine()
        })
    return _DBSession


def init_database() -> None:
    """初始化数据库并执行待处理的迁移"""
    from pathlib import Path

    config = get_config()
    os.makedirs(config.database_dir, exist_ok=True)

    history_db_path = Path(config.history_database_file)
    is_new_database = not history_db_path.exists()

    # 初始化主数据库
    Base.metadata.create_all(bind=_get_engine())
    # 初始化历史数据库（创建基础表结构）
    HistoryBase.metadata.create_all(bind=_get_history_engine())

    # 如果是全新数据库，不需要迁移（字段已通过 create_all 创建）
    if is_new_database:
        # 记录初始版本为最新迁移版本
        from flowy.core.migration_manager import get_migration_manager
        manager = get_migration_manager()
        if manager.get_migration_files():
            manager.create_migrations_table()
            # 标记所有迁移为已执行
            for version, migration_cls in sorted(manager.get_migration_files().items()):
                migration = migration_cls()
                manager.record_migration(None, migration)
    else:
        # 现有数据库，执行待处理的迁移
        run_pending_migrations()


def get_session() -> Session:
    """获取数据库会话"""
    return _get_session_maker()()


def create_flow_history(
        session: Session,
        flow_id: str,
        created_at: datetime,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        input_data: Optional[str] = None,
        output_data: Optional[str] = None,
        status: str = 'pending',
        flow_metadata: Optional[str] = None
) -> FlowHistory:
    """创建FlowHistory记录"""
    flow_history = FlowHistory(
        flow_metadata=flow_metadata or "",
        created_at=created_at,
        start_time=start_time,
        end_time=end_time,
        input_data=input_data,
        output_data=output_data,
        status=status,
        flow_id=flow_id,
    )
    session.add(flow_history)
    return flow_history


def update_flow_history(
        session: Session,
        history_id: int,
        end_time: Optional[datetime] = None,
        output_data: Optional[str] = None,
        status: Optional[str] = None
) -> Optional[FlowHistory]:
    """更新FlowHistory记录"""
    flow_history = session.query(FlowHistory).filter(FlowHistory.id == history_id).first()
    if flow_history:
        if end_time is not None:
            flow_history.end_time = end_time
        if output_data is not None:
            flow_history.output_data = output_data
        if status is not None:
            flow_history.status = status
    return flow_history


def register_flow(
        session: Session,
        flow_id: str,
        name: str,
        desc: Optional[str] = None):
    """注册Flow"""
    flow = session.query(Flow).filter(Flow.id == flow_id).first()
    if flow:
        flow.name = name
        flow.description = desc or flow.description
    else:
        flow = Flow(
            id=flow_id,
            name=name,
            description=desc
        )
        session.add(flow)
    return flow


def create_task_history(
        session: Session,
        flow_history_id: int,
        name: str,
        created_at: datetime,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        input_data: Optional[str] = None,
        output_data: Optional[str] = None,
        status: str = 'pending'
) -> TaskHistory:
    """创建TaskHistory记录"""
    task_history = TaskHistory(
        flow_history_id=flow_history_id,
        name=name,
        created_at=created_at,
        start_time=start_time,
        end_time=end_time,
        input_data=input_data,
        output_data=output_data,
        status=status
    )
    session.add(task_history)
    return task_history


def update_task_history(
        session: Session,
        task_id: int,
        end_time: Optional[datetime] = None,
        output_data: Optional[str] = None,
        status: Optional[str] = None
) -> Optional[TaskHistory]:
    """更新TaskHistory记录"""
    task_history = session.query(TaskHistory).filter(TaskHistory.id == task_id).first()
    if task_history:
        if end_time is not None:
            task_history.end_time = end_time
        if output_data is not None:
            task_history.output_data = output_data
        if status is not None:
            task_history.status = status
    return task_history


def update_task_progress(
        session: Session,
        task_id: int,
        progress: int,
        progress_message: Optional[str] = None
) -> Optional[TaskHistory]:
    """更新任务进度

    Args:
        session: 数据库会话
        task_id: 任务ID
        progress: 进度百分比 (0-100)
        progress_message: 进度描述信息（可选）

    Returns:
        更新后的 TaskHistory 对象，如果任务不存在则返回 None
    """
    task_history = session.query(TaskHistory).filter(TaskHistory.id == task_id).first()
    if task_history:
        task_history.progress = progress
        task_history.progress_message = progress_message
        task_history.progress_updated_at = datetime.now()
    return task_history


def get_running_tasks(
        session: Session,
        flow_history_id: int
) -> list[TaskHistory]:
    """获取指定流程历史中正在运行的任务

    Args:
        session: 数据库会话
        flow_history_id: 流程历史ID

    Returns:
        正在运行的任务列表
    """
    return session.query(TaskHistory).filter(
        TaskHistory.flow_history_id == flow_history_id,
        TaskHistory.status == 'running'
    ).all()


# ============================================
# 数据库迁移便捷函数
# ============================================

def run_pending_migrations(silent: bool = False) -> tuple[int, int, list[str]]:
    """执行待处理的数据库迁移

    Args:
        silent: 是否静默模式（不打印输出）

    Returns:
        (成功数量, 失败数量, 错误信息列表)
    """
    from flowy.core.migration_manager import run_pending_migrations as _run

    if not silent:
        from flowy.core.config import get_config
        config = get_config()
        print(f'检查数据库迁移: {config.history_database_file}')

    success, failed, errors = _run()

    if not silent:
        if success > 0:
            print(f'成功执行 {success} 个迁移')
        if failed > 0:
            print(f'迁移失败: {", ".join(errors)}')

    return success, failed, errors


def get_migration_history() -> list[dict]:
    """获取迁移历史记录

    Returns:
        迁移历史列表，每个元素包含 version, name, description, applied_at
    """
    from flowy.core.migration_manager import get_migration_manager
    manager = get_migration_manager()
    return manager.get_migration_history()


def get_current_db_version() -> str | None:
    """获取当前数据库版本

    Returns:
        当前数据库版本号，如果数据库不存在或未执行过迁移则返回 None
    """
    from flowy.core.migration_manager import get_migration_manager
    manager = get_migration_manager()
    return manager.get_current_version()


# ============================================
# 备注相关功能
# ============================================

def add_flow_remark(
        session: Session,
        history_id: int,
        level: str,
        message: str
) -> bool:
    """为Flow执行历史添加备注

    Args:
        session: 数据库会话
        history_id: 执行历史ID
        level: 备注级别 (info/warning/error)
        message: 备注消息

    Returns:
        是否添加成功
    """
    from flowy.core.json_utils import json

    if level not in ('info', 'warning', 'error'):
        raise ValueError(f"无效的备注级别: {level}，必须是 info、warning 或 error")

    flow_history = session.query(FlowHistory).filter(
        FlowHistory.id == history_id
    ).first()

    if not flow_history:
        return False

    # 解析现有的 metadata
    try:
        metadata = json.safe_loads(flow_history.flow_metadata or '{}')
    except (ValueError, TypeError):
        metadata = {}

    # 获取或创建备注列表
    remarks = metadata.get('remarks', [])

    # 添加新备注
    remark = {
        'level': level,
        'message': message,
        'timestamp': datetime.now().isoformat()
    }
    remarks.append(remark)

    # 更新 metadata
    metadata['remarks'] = remarks
    flow_history.flow_metadata = json.dumps(metadata)

    # 提交更改到数据库
    session.commit()

    return True


def get_flow_remarks(session: Session, history_id: int) -> list:
    """获取Flow执行历史的备注列表

    Args:
        session: 数据库会话
        history_id: 执行历史ID

    Returns:
        备注列表，每个元素包含 level, message, timestamp
    """
    from flowy.core.json_utils import json

    flow_history = session.query(FlowHistory).filter(
        FlowHistory.id == history_id
    ).first()

    if not flow_history:
        return []

    try:
        metadata = json.safe_loads(flow_history.flow_metadata or '{}')
        return metadata.get('remarks', [])
    except (ValueError, TypeError):
        return []
