#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy数据库模块"""

import os
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, create_engine, DateTime, Text, INTEGER
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from flowy.core.config import get_config

Base = declarative_base()


class Trigger(Base):
    __tablename__ = 'trigger'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    flow_id = Column(String(64))
    name = Column(String(64))
    description = Column(String(256))
    cron_expression = Column(String(128))
    trigger_params = Column(Text)
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


class FlowHistory(Base):
    __tablename__ = 'flow_history'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    flow_metadata = Column(Text, default="")
    flow_id = Column(String(64))
    created_at = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    input_data = Column(Text)
    output_data = Column(Text)
    status = Column(String(64))


class TaskHistory(Base):
    __tablename__ = 'task_history'
    id = Column(INTEGER, primary_key=True)
    flow_history_id = Column(INTEGER)
    name = Column(String(64))
    created_at = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    input_data = Column(Text)
    output_data = Column(Text)
    status = Column(String(64))


# 延迟初始化的数据库引擎和会话
_engine = None
_DBSession = None


def _get_engine():
    """获取数据库引擎（延迟初始化）"""
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


def _get_session_maker():
    """获取会话工厂（延迟初始化）"""
    global _DBSession
    if _DBSession is None:
        _DBSession = sessionmaker(bind=_get_engine())
    return _DBSession


def init_database() -> None:
    """初始化数据库"""
    config = get_config()
    os.makedirs(config.database_dir, exist_ok=True)
    Base.metadata.create_all(bind=_get_engine())


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
