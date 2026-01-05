#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""触发器服务模块"""

import logging
from datetime import datetime
from typing import List, Optional

from flowy.core.json_utils import json
from apscheduler.triggers.cron import CronTrigger

from flowy.core.db import get_session, Trigger, Flow
from flowy.web.services.scheduler_service import SchedulerService

logger = logging.getLogger(__name__)


class TriggerService:
    """触发器服务类"""
    
    @staticmethod
    def create_trigger(
        flow_id: str,
        name: str,
        description: str,
        cron_expression: str,
        trigger_params: dict,
        max_instances: int = 1
    ) -> Trigger:
        """创建触发器

        Args:
            flow_id: 工作流ID
            name: 触发器名称
            description: 触发器描述
            cron_expression: Cron表达式
            trigger_params: 触发参数字典
            max_instances: 最大并发实例数，默认1

        Returns:
            创建的触发器对象

        Raises:
            ValueError: 当工作流不存在或Cron表达式无效时
        """
        session = get_session()
        try:
            # 验证工作流存在
            flow = session.query(Flow).filter(Flow.id == flow_id).first()
            if not flow:
                raise ValueError(f"工作流 {flow_id} 不存在")

            # 验证 Cron 表达式
            try:
                CronTrigger.from_crontab(cron_expression)
            except Exception as e:
                raise ValueError(f"无效的 Cron 表达式: {e}")

            # 验证 max_instances
            if max_instances < 1:
                raise ValueError("max_instances 必须大于等于 1")

            # 创建触发器
            trigger = Trigger(
                flow_id=flow_id,
                name=name,
                description=description,
                cron_expression=cron_expression,
                trigger_params=json.dumps(trigger_params, ensure_ascii=False),
                max_instances=max_instances,
                enabled=1,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )

            session.add(trigger)
            session.commit()
            session.refresh(trigger)

            # 添加到调度器
            SchedulerService.add_job(
                trigger.id,
                trigger.flow_id,
                trigger.cron_expression,
                max_instances=max_instances
            )

            logger.info(f"创建触发器成功: {trigger.id} - {trigger.name}, max_instances: {max_instances}")
            return trigger
        finally:
            session.close()
    
    @staticmethod
    def get_triggers_by_flow(flow_id: str) -> List[Trigger]:
        """获取指定工作流的触发器列表
        
        Args:
            flow_id: 工作流ID
            
        Returns:
            触发器列表，按创建时间倒序排列
        """
        session = get_session()
        try:
            from sqlalchemy import desc
            return session.query(Trigger).filter(
                Trigger.flow_id == flow_id
            ).order_by(desc(Trigger.created_at)).all()
        finally:
            session.close()
    
    @staticmethod
    def get_trigger_by_id(trigger_id: int) -> Optional[Trigger]:
        """根据ID获取触发器
        
        Args:
            trigger_id: 触发器ID
            
        Returns:
            触发器对象，如果不存在则返回None
        """
        session = get_session()
        try:
            return session.query(Trigger).filter(
                Trigger.id == trigger_id
            ).first()
        finally:
            session.close()
    
    @staticmethod
    def update_trigger(
        trigger_id: int,
        name: str = None,
        description: str = None,
        cron_expression: str = None,
        trigger_params: dict = None,
        max_instances: int = None
    ) -> Trigger:
        """更新触发器

        Args:
            trigger_id: 触发器ID
            name: 新的触发器名称（可选）
            description: 新的触发器描述（可选）
            cron_expression: 新的Cron表达式（可选）
            trigger_params: 新的触发参数（可选）
            max_instances: 新的最大实例数（可选）

        Returns:
            更新后的触发器对象

        Raises:
            ValueError: 当触发器不存在或Cron表达式无效时
        """
        session = get_session()
        try:
            trigger = session.query(Trigger).filter(
                Trigger.id == trigger_id
            ).first()

            if not trigger:
                raise ValueError(f"触发器 {trigger_id} 不存在")

            # 验证新的 Cron 表达式
            if cron_expression:
                try:
                    CronTrigger.from_crontab(cron_expression)
                except Exception as e:
                    raise ValueError(f"无效的 Cron 表达式: {e}")
                trigger.cron_expression = cron_expression

            if name:
                trigger.name = name
            if description is not None:
                trigger.description = description
            if trigger_params is not None:
                trigger.trigger_params = json.dumps(trigger_params, ensure_ascii=False)
            if max_instances is not None:
                if max_instances < 1:
                    raise ValueError("max_instances 必须大于等于 1")
                trigger.max_instances = max_instances

            trigger.updated_at = datetime.now()
            session.commit()
            session.refresh(trigger)

            # 重新加载调度任务
            if trigger.enabled:
                SchedulerService.add_job(
                    trigger.id,
                    trigger.flow_id,
                    trigger.cron_expression,
                    max_instances=trigger.max_instances
                )

            logger.info(f"更新触发器成功: {trigger.id} - {trigger.name}, max_instances: {trigger.max_instances}")
            return trigger
        finally:
            session.close()
    
    @staticmethod
    def delete_trigger(trigger_id: int):
        """删除触发器
        
        Args:
            trigger_id: 触发器ID
            
        Raises:
            ValueError: 当触发器不存在时
        """
        session = get_session()
        try:
            trigger = session.query(Trigger).filter(
                Trigger.id == trigger_id
            ).first()
            
            if not trigger:
                raise ValueError(f"触发器 {trigger_id} 不存在")
            
            # 从调度器移除
            SchedulerService.remove_job(trigger_id)
            
            # 从数据库删除
            session.delete(trigger)
            session.commit()
            
            logger.info(f"删除触发器成功: {trigger_id}")
        finally:
            session.close()
    
    @staticmethod
    def toggle_trigger_status(trigger_id: int, enabled: bool):
        """切换触发器启用状态
        
        Args:
            trigger_id: 触发器ID
            enabled: 是否启用
            
        Raises:
            ValueError: 当触发器不存在时
        """
        session = get_session()
        try:
            trigger = session.query(Trigger).filter(
                Trigger.id == trigger_id
            ).first()
            
            if not trigger:
                raise ValueError(f"触发器 {trigger_id} 不存在")
            
            trigger.enabled = 1 if enabled else 0
            trigger.updated_at = datetime.now()
            session.commit()
            
            # 更新调度器
            if enabled:
                SchedulerService.resume_job(trigger_id)
            else:
                SchedulerService.pause_job(trigger_id)
            
            logger.info(f"切换触发器状态成功: {trigger_id} - enabled={enabled}")
        finally:
            session.close()
