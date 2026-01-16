#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Payload 存储模块 - 独立存储 input/output 数据"""

import os
import zlib
from typing import Optional, Dict, Any

from sqlalchemy import Column, INTEGER, LargeBinary, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from flowy.core.config import get_config
from flowy.core.json_utils import json

PayloadBase = declarative_base()


class CompressedData(LargeBinary):
    """压缩数据列类型"""
    pass


class FlowPayload(PayloadBase):
    """Flow 执行历史的 payload 数据"""
    __tablename__ = 'flow_payload'
    
    history_id = Column(INTEGER, primary_key=True)
    input_data = Column(CompressedData)
    output_data = Column(CompressedData)


class TaskPayload(PayloadBase):
    """Task 执行历史的 payload 数据"""
    __tablename__ = 'task_payload'
    
    task_id = Column(INTEGER, primary_key=True)
    input_data = Column(CompressedData)
    output_data = Column(CompressedData)


# 延迟初始化
_payload_engine = None
_PayloadSession = None


def _get_payload_engine():
    """获取 payload 数据库引擎"""
    global _payload_engine
    if _payload_engine is None:
        config = get_config()
        os.makedirs(config.database_dir, exist_ok=True)
        payload_db_path = os.path.join(config.database_dir, 'flowy_payload.db')
        _payload_engine = create_engine(
            f'sqlite:///{payload_db_path}',
            echo=False,
            pool_pre_ping=True,
            connect_args={
                'check_same_thread': False,
                'timeout': 20
            }
        )
    return _payload_engine


def _get_payload_session() -> Session:
    """获取 payload 数据库会话"""
    global _PayloadSession
    if _PayloadSession is None:
        _PayloadSession = sessionmaker(bind=_get_payload_engine())
    return _PayloadSession()


def init_payload_database():
    """初始化 payload 数据库"""
    PayloadBase.metadata.create_all(bind=_get_payload_engine())


def _compress(data: str) -> bytes:
    """压缩字符串数据"""
    if data is None:
        return None
    return zlib.compress(data.encode('utf-8'), level=9)


def _decompress(data: bytes) -> str:
    """解压缩数据"""
    if data is None:
        return None
    return zlib.decompress(data).decode('utf-8')


# ============================================
# Flow Payload 操作
# ============================================

def save_flow_payload(history_id: int, input_data: Optional[str] = None, 
                      output_data: Optional[str] = None) -> None:
    """保存 Flow 的 payload 数据"""
    session = _get_payload_session()
    try:
        payload = session.query(FlowPayload).filter(
            FlowPayload.history_id == history_id
        ).first()
        
        if payload is None:
            payload = FlowPayload(history_id=history_id)
            session.add(payload)
        
        if input_data is not None:
            payload.input_data = _compress(input_data)
        if output_data is not None:
            payload.output_data = _compress(output_data)
        
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_flow_input(history_id: int) -> Optional[Dict[str, Any]]:
    """获取 Flow 的输入数据"""
    session = _get_payload_session()
    try:
        payload = session.query(FlowPayload).filter(
            FlowPayload.history_id == history_id
        ).first()
        
        if payload is None or payload.input_data is None:
            return None
        
        return json.safe_loads(_decompress(payload.input_data))
    finally:
        session.close()


def get_flow_output(history_id: int) -> Optional[Dict[str, Any]]:
    """获取 Flow 的输出数据"""
    session = _get_payload_session()
    try:
        payload = session.query(FlowPayload).filter(
            FlowPayload.history_id == history_id
        ).first()
        
        if payload is None or payload.output_data is None:
            return None
        
        return json.safe_loads(_decompress(payload.output_data))
    finally:
        session.close()


def delete_flow_payload(history_id: int) -> bool:
    """删除 Flow 的 payload 数据"""
    session = _get_payload_session()
    try:
        result = session.query(FlowPayload).filter(
            FlowPayload.history_id == history_id
        ).delete()
        session.commit()
        return result > 0
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def batch_delete_flow_payload(history_ids: list) -> int:
    """批量删除 Flow 的 payload 数据"""
    if not history_ids:
        return 0
    session = _get_payload_session()
    try:
        result = session.query(FlowPayload).filter(
            FlowPayload.history_id.in_(history_ids)
        ).delete(synchronize_session=False)
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ============================================
# Task Payload 操作
# ============================================

def save_task_payload(task_id: int, input_data: Optional[str] = None,
                      output_data: Optional[str] = None) -> None:
    """保存 Task 的 payload 数据"""
    session = _get_payload_session()
    try:
        payload = session.query(TaskPayload).filter(
            TaskPayload.task_id == task_id
        ).first()
        
        if payload is None:
            payload = TaskPayload(task_id=task_id)
            session.add(payload)
        
        if input_data is not None:
            payload.input_data = _compress(input_data)
        if output_data is not None:
            payload.output_data = _compress(output_data)
        
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_task_input(task_id: int) -> Optional[Dict[str, Any]]:
    """获取 Task 的输入数据"""
    session = _get_payload_session()
    try:
        payload = session.query(TaskPayload).filter(
            TaskPayload.task_id == task_id
        ).first()
        
        if payload is None or payload.input_data is None:
            return None
        
        return json.safe_loads(_decompress(payload.input_data))
    finally:
        session.close()


def get_task_output(task_id: int) -> Optional[Dict[str, Any]]:
    """获取 Task 的输出数据"""
    session = _get_payload_session()
    try:
        payload = session.query(TaskPayload).filter(
            TaskPayload.task_id == task_id
        ).first()
        
        if payload is None or payload.output_data is None:
            return None
        
        return json.safe_loads(_decompress(payload.output_data))
    finally:
        session.close()


def get_task_data(task_id: int) -> Optional[Dict[str, Any]]:
    """获取 Task 的完整 payload 数据"""
    session = _get_payload_session()
    try:
        payload = session.query(TaskPayload).filter(
            TaskPayload.task_id == task_id
        ).first()
        
        if payload is None:
            return None
        
        result = {}
        if payload.input_data:
            result['input_data'] = json.safe_loads(_decompress(payload.input_data))
        if payload.output_data:
            result['output_data'] = json.safe_loads(_decompress(payload.output_data))
        
        return result
    finally:
        session.close()


def delete_task_payload(task_id: int) -> bool:
    """删除 Task 的 payload 数据"""
    session = _get_payload_session()
    try:
        result = session.query(TaskPayload).filter(
            TaskPayload.task_id == task_id
        ).delete()
        session.commit()
        return result > 0
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def batch_delete_task_payload(task_ids: list) -> int:
    """批量删除 Task 的 payload 数据"""
    if not task_ids:
        return 0
    session = _get_payload_session()
    try:
        result = session.query(TaskPayload).filter(
            TaskPayload.task_id.in_(task_ids)
        ).delete(synchronize_session=False)
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def delete_task_payloads_by_flow(flow_history_id: int, task_ids: list) -> int:
    """根据 flow_history_id 关联的 task_ids 删除 payload"""
    return batch_delete_task_payload(task_ids)
