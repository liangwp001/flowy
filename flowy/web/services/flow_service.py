#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flow业务逻辑服务层"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime

from flowy.core.json_utils import json
from sqlalchemy import func, desc, asc, and_

from flowy.core.db import Flow, FlowHistory, TaskHistory, get_session, get_running_tasks, get_flow_remarks


class FlowService:
    """Flow服务类"""

    @staticmethod
    def get_all_flows_with_stats() -> List[Dict]:
        """获取所有Flow及其统计信息（用于侧边栏）
        
        优化：使用单个会话完成所有查询，避免嵌套会话导致的连接泄露
        """
        session = get_session()
        try:
            flows = session.query(Flow).order_by(Flow.name).all()

            flows_with_stats = []
            for flow in flows:
                # 在同一个会话中获取统计信息，避免创建新会话
                stats = FlowService._get_flow_statistics_with_session(session, flow.id)
                
                # 分离对象
                session.expunge(flow)
                
                flows_with_stats.append({
                    'flow': flow,
                    'stats': stats
                })

            return flows_with_stats
        finally:
            session.close()

    @staticmethod
    def _get_flow_statistics_with_session(session, flow_id: str) -> Dict:
        """使用现有会话获取Flow的统计信息（内部方法，避免创建新会话）"""
        total_count = session.query(func.count(FlowHistory.id)).filter(
            FlowHistory.flow_id == flow_id
        ).scalar() or 0

        success_count = session.query(func.count(FlowHistory.id)).filter(
            FlowHistory.flow_id == flow_id,
            FlowHistory.status == 'completed'
        ).scalar() or 0

        failed_count = session.query(func.count(FlowHistory.id)).filter(
            FlowHistory.flow_id == flow_id,
            FlowHistory.status == 'failed'
        ).scalar() or 0

        running_count = session.query(func.count(FlowHistory.id)).filter(
            FlowHistory.flow_id == flow_id,
            FlowHistory.status == 'running'
        ).scalar() or 0

        pending_count = session.query(func.count(FlowHistory.id)).filter(
            FlowHistory.flow_id == flow_id,
            FlowHistory.status == 'pending'
        ).scalar() or 0

        success_rate = (success_count / total_count * 100) if total_count > 0 else 0

        # 计算平均执行时长 - 使用聚合查询而非加载所有记录
        avg_duration_result = session.query(
            func.avg(
                func.julianday(FlowHistory.end_time) - func.julianday(FlowHistory.start_time)
            ) * 86400  # 转换为秒
        ).filter(
            FlowHistory.flow_id == flow_id,
            FlowHistory.status == 'completed',
            FlowHistory.start_time.isnot(None),
            FlowHistory.end_time.isnot(None)
        ).scalar()
        
        avg_duration = round(avg_duration_result, 1) if avg_duration_result else 0

        # 计算平均等待时长
        avg_wait_result = session.query(
            func.avg(
                func.julianday(FlowHistory.start_time) - func.julianday(FlowHistory.created_at)
            ) * 86400
        ).filter(
            FlowHistory.flow_id == flow_id,
            FlowHistory.status.in_(['pending', 'running']),
            FlowHistory.start_time.isnot(None)
        ).scalar()
        
        avg_wait_time = round(avg_wait_result, 1) if avg_wait_result else 0

        latest_history = session.query(FlowHistory).filter(
            FlowHistory.flow_id == flow_id
        ).order_by(desc(FlowHistory.created_at)).first()

        latest_status = latest_history.status if latest_history else None
        latest_execution = latest_history.created_at if latest_history else None

        return {
            'total_count': total_count,
            'success_count': success_count,
            'failed_count': failed_count,
            'running_count': running_count,
            'pending_count': pending_count,
            'success_rate': round(success_rate, 2),
            'avg_duration': avg_duration,
            'avg_wait_time': avg_wait_time,
            'latest_status': latest_status,
            'latest_execution': latest_execution
        }

    @staticmethod
    def get_flows_paginated(page: int = 1, per_page: int = 20, search: str = None) -> Tuple[List[Flow], int]:
        """获取分页的Flow列表"""
        session = get_session()
        try:
            query = session.query(Flow)

            if search:
                search = f"%{search}%"
                query = query.filter(
                    func.or_(
                        Flow.name.like(search),
                        Flow.description.like(search)
                    )
                )

            query = query.order_by(desc(Flow.created_at))
            total = query.count()
            flows = query.offset((page - 1) * per_page).limit(per_page).all()

            return flows, (total + per_page - 1) // per_page
        finally:
            session.close()

    @staticmethod
    def get_flow_by_id(flow_id: str) -> Optional[Flow]:
        """根据ID获取Flow"""
        session = get_session()
        try:
            return session.query(Flow).filter(Flow.id == flow_id).first()
        finally:
            session.close()

    @staticmethod
    def get_flow_statistics(flow_id: str) -> Dict:
        """获取Flow的统计信息
        
        优化：使用聚合查询代替加载所有记录到内存
        """
        session = get_session()
        try:
            return FlowService._get_flow_statistics_with_session(session, flow_id)
        finally:
            session.close()

    @staticmethod
    def get_flow_history_paginated(flow_id: str, page: int = 1, per_page: int = 30,
                                   status_filter: str = None) -> Tuple[List[FlowHistory], int]:
        """获取Flow执行历史分页列表"""
        session = get_session()
        try:
            query = session.query(FlowHistory).filter(FlowHistory.flow_id == flow_id)

            if status_filter:
                query = query.filter(FlowHistory.status == status_filter)

            query = query.order_by(desc(FlowHistory.created_at))
            total = query.count()
            histories = query.offset((page - 1) * per_page).limit(per_page).all()

            # 解析JSON数据并为运行中的历史记录获取正在运行的任务
            for history in histories:
                # 先从会话中分离对象，避免 autoflush 问题
                session.expunge(history)

                try:
                    history.input_data = json.safe_loads(history.input_data or '{}')
                except (ValueError, TypeError):
                    history.input_data = {}

                # 输出数据可能是双重编码的，需要尝试双重解析
                try:
                    parsed_output = json.safe_loads(history.output_data or '{}')
                    # 如果解析后仍然是字符串，再解析一次
                    if isinstance(parsed_output, str):
                        history.output_data = json.safe_loads(parsed_output)
                    else:
                        history.output_data = parsed_output
                except (ValueError, TypeError):
                    history.output_data = {}

                # 解析触发器信息和备注
                history.trigger_name = None
                history.trigger_type = None
                history.remarks = []
                try:
                    metadata = json.safe_loads(history.flow_metadata or '{}')
                    history.trigger_name = metadata.get('trigger_name')
                    history.trigger_type = metadata.get('trigger_type')
                    history.remarks = metadata.get('remarks', [])
                except (ValueError, TypeError):
                    pass

                # 计算备注的最高级别（用于前端显示图标颜色）
                history.remark_level = None
                if history.remarks:
                    # 优先级：error > warning > info
                    if any(r['level'] == 'error' for r in history.remarks):
                        history.remark_level = 'error'
                    elif any(r['level'] == 'warning' for r in history.remarks):
                        history.remark_level = 'warning'
                    else:
                        history.remark_level = 'info'

                # 对于运行中的历史记录，获取正在运行的任务及其进度
                if history.status == 'running':
                    running_tasks = get_running_tasks(session, history.id)
                    history.running_tasks = [
                        {
                            'id': t.id,
                            'name': t.name,
                            'progress': t.progress,
                            'progress_message': t.progress_message,
                            'progress_updated_at': t.progress_updated_at.isoformat() if t.progress_updated_at else None
                        }
                        for t in running_tasks
                    ]
                else:
                    # 对于非运行状态，获取最后一个任务用于展示
                    last_task = session.query(TaskHistory).filter(
                        TaskHistory.flow_history_id == history.id
                    ).order_by(desc(TaskHistory.created_at)).first()
                    if last_task:
                        history.running_tasks = [
                            {
                                'id': last_task.id,
                                'name': last_task.name,
                                'status': last_task.status,
                                'progress': None,
                                'progress_message': None
                            }
                        ]
                    else:
                        history.running_tasks = []

            return histories, (total + per_page - 1) // per_page
        finally:
            session.close()

    @staticmethod
    def get_flow_history_detail(history_id: int) -> Optional[Dict]:
        """获取执行历史详情"""
        session = get_session()
        try:
            history = session.query(FlowHistory).filter(
                FlowHistory.id == history_id
            ).first()

            if not history:
                return None

            # 获取Flow信息
            flow = session.query(Flow).filter(
                Flow.id == history.flow_id
            ).first()

            task_histories = session.query(TaskHistory).filter(
                TaskHistory.flow_history_id == history_id
            ).order_by(asc(TaskHistory.created_at)).all()

            session.expunge(history)
            if flow:
                session.expunge(flow)
            for task in task_histories:
                session.expunge(task)

            try:
                history.input_data = json.safe_loads(history.input_data or '{}')
            except (ValueError, TypeError):
                history.input_data = {}

            try:
                history.output_data = json.safe_loads(history.output_data or '{}')
            except (ValueError, TypeError):
                history.output_data = {}

            # 解析备注信息
            history.remarks = []
            history.remark_level = None
            try:
                metadata = json.safe_loads(history.flow_metadata or '{}')
                history.remarks = metadata.get('remarks', [])
                # 计算备注的最高级别
                if history.remarks:
                    if any(r['level'] == 'error' for r in history.remarks):
                        history.remark_level = 'error'
                    elif any(r['level'] == 'warning' for r in history.remarks):
                        history.remark_level = 'warning'
                    else:
                        history.remark_level = 'info'
            except (ValueError, TypeError):
                pass

            for task in task_histories:
                try:
                    task.input_data = json.safe_loads(task.input_data or '{}')
                except (ValueError, TypeError):
                    task.input_data = {}

                try:
                    task.output_data = json.safe_loads(task.output_data or '{}')
                except (ValueError, TypeError):
                    task.output_data = {}

            return {
                'flow_history': history,
                'task_histories': task_histories,
                'flow': flow
            }
        finally:
            session.close()



    @staticmethod
    def delete_flow_history(history_id: int) -> bool:
        """删除单条执行历史"""
        session = get_session()
        try:
            history = session.query(FlowHistory).filter(
                FlowHistory.id == history_id
            ).first()

            if not history:
                return False

            # 删除关联的任务历史
            session.query(TaskHistory).filter(
                TaskHistory.flow_history_id == history_id
            ).delete()

            # 删除执行历史
            session.delete(history)
            session.commit()
            return True
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def batch_delete_flow_history(history_ids: List[int]) -> Tuple[int, int]:
        """批量删除执行历史
        返回 (成功数量, 失败数量)
        """
        session = get_session()
        success_count = 0
        failed_count = 0
        try:
            for history_id in history_ids:
                try:
                    history = session.query(FlowHistory).filter(
                        FlowHistory.id == history_id
                    ).first()

                    if history:
                        # 删除关联的任务历史
                        session.query(TaskHistory).filter(
                            TaskHistory.flow_history_id == history_id
                        ).delete()
                        # 删除执行历史
                        session.delete(history)
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception:
                    failed_count += 1

            session.commit()
            return success_count, failed_count
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_task_histories(history_id: int) -> List[TaskHistory]:
        """获取执行历史的任务列表"""
        session = get_session()
        try:
            task_histories = session.query(TaskHistory).filter(
                TaskHistory.flow_history_id == history_id
            ).order_by(asc(TaskHistory.created_at)).all()

            # 分离对象以避免会话问题
            for task in task_histories:
                session.expunge(task)

            return task_histories
        finally:
            session.close()

    @staticmethod
    def get_task_data(task_id: int) -> Optional[Dict]:
        """获取任务的输入输出数据"""
        from flowy.core.json_utils import json

        session = get_session()
        try:
            task = session.query(TaskHistory).filter(
                TaskHistory.id == task_id
            ).first()

            if not task:
                return None

            session.expunge(task)

            # 解析 JSON 数据
            input_data = None
            output_data = None

            if task.input_data:
                try:
                    input_data = json.safe_loads(task.input_data)
                except (ValueError, TypeError):
                    input_data = task.input_data

            if task.output_data:
                try:
                    output_data = json.safe_loads(task.output_data)
                except (ValueError, TypeError):
                    output_data = task.output_data

            return {
                'id': task.id,
                'name': task.name,
                'status': task.status,
                'input_data': input_data,
                'output_data': output_data,
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'start_time': task.start_time.isoformat() if task.start_time else None,
                'end_time': task.end_time.isoformat() if task.end_time else None
            }
        finally:
            session.close()

    @staticmethod
    def get_flow_chart_data(flow_id: str, days: int = 30) -> Dict:
        """获取Flow图表数据"""
        from datetime import timedelta
        session = get_session()
        try:
            # 获取指定天数内的执行记录
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            histories = session.query(FlowHistory).filter(
                FlowHistory.flow_id == flow_id,
                FlowHistory.created_at >= start_date,
                FlowHistory.created_at <= end_date
            ).order_by(FlowHistory.created_at).all()

            # 按日期分组统计
            daily_stats = {}
            status_over_time = {
                'completed': [],
                'failed': [],
                'pending': [],
                'running': []
            }

            # 初始化日期范围
            current_date = start_date.date()
            while current_date <= end_date.date():
                daily_stats[current_date.strftime('%Y-%m-%d')] = {
                    'total': 0,
                    'success': 0,
                    'failed': 0,
                    'pending': 0,
                    'running': 0
                }
                current_date += timedelta(days=1)

            # 统计每日数据
            for history in histories:
                date_key = history.created_at.strftime('%Y-%m-%d')
                if date_key in daily_stats:
                    daily_stats[date_key]['total'] += 1
                    if history.status == 'completed':
                        daily_stats[date_key]['success'] += 1
                    elif history.status == 'failed':
                        daily_stats[date_key]['failed'] += 1
                    elif history.status == 'pending':
                        daily_stats[date_key]['pending'] += 1
                    elif history.status == 'running':
                        daily_stats[date_key]['running'] += 1

            # 准备图表数据
            dates = list(daily_stats.keys())
            success_data = [daily_stats[d]['success'] for d in dates]
            failed_data = [daily_stats[d]['failed'] for d in dates]

            # 执行时长分布
            durations = []
            for h in histories:
                if h.status == 'completed' and h.start_time and h.end_time:
                    duration = (h.end_time - h.start_time).total_seconds() / 60  # 转换为分钟
                    durations.append(duration)

            # 按时长分组
            duration_ranges = {
                '< 1分钟': 0,
                '1-5分钟': 0,
                '5-15分钟': 0,
                '15-60分钟': 0,
                '> 60分钟': 0
            }

            for duration in durations:
                if duration < 1:
                    duration_ranges['< 1分钟'] += 1
                elif duration < 5:
                    duration_ranges['1-5分钟'] += 1
                elif duration < 15:
                    duration_ranges['5-15分钟'] += 1
                elif duration < 60:
                    duration_ranges['15-60分钟'] += 1
                else:
                    duration_ranges['> 60分钟'] += 1

            # 每小时执行分布（最近7天，按小时统计成功和失败）
            hourly_stats = {}
            for hour in range(24):
                hourly_stats[hour] = {'success': 0, 'failed': 0, 'pending': 0, 'running': 0, 'total': 0}

            # 获取最近7天的执行记录，按小时统计
            recent_histories = session.query(FlowHistory).filter(
                FlowHistory.flow_id == flow_id,
                FlowHistory.created_at >= datetime.now() - timedelta(days=7)
            ).all()

            for history in recent_histories:
                hour = history.created_at.hour
                hourly_stats[hour]['total'] += 1
                if history.status == 'completed':
                    hourly_stats[hour]['success'] += 1
                elif history.status == 'failed':
                    hourly_stats[hour]['failed'] += 1
                elif history.status == 'pending':
                    hourly_stats[hour]['pending'] += 1
                elif history.status == 'running':
                    hourly_stats[hour]['running'] += 1

            return {
                'daily_trend': {
                    'dates': dates,
                    'success': success_data,
                    'failed': failed_data
                },
                'duration_distribution': {
                    'labels': list(duration_ranges.keys()),
                    'data': list(duration_ranges.values())
                },
                'hourly_distribution': {
                    'hours': [f"{hour}:00" for hour in hourly_stats.keys()],
                    'success': [hourly_stats[hour]['success'] for hour in hourly_stats.keys()],
                    'failed': [hourly_stats[hour]['failed'] for hour in hourly_stats.keys()],
                    'pending': [hourly_stats[hour]['pending'] for hour in hourly_stats.keys()],
                    'running': [hourly_stats[hour]['running'] for hour in hourly_stats.keys()],
                    'total': [hourly_stats[hour]['total'] for hour in hourly_stats.keys()]
                }
            }
        finally:
            session.close()

  