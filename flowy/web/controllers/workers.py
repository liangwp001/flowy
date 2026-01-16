#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Worker管理API控制器"""

import json
from flask import Blueprint, jsonify, request, render_template
from flowy.core.db import get_session, Worker, FlowHistory

# 创建两个蓝图：一个用于页面路由，一个用于API路由
workers_bp = Blueprint('workers', __name__)


# ============ 页面路由 ============

@workers_bp.route('/workers')
def workers_page():
    """Worker管理页面
    
    显示所有Worker的列表，包括状态、标签、心跳时间等信息。
    支持draining、释放任务、删除等操作。
    
    Requirements: 10.1, 10.2
    """
    session = get_session()
    try:
        workers = session.query(Worker).order_by(Worker.registered_at.desc()).all()
        
        # 为每个Worker获取任务统计
        workers_data = []
        for w in workers:
            # 统计任务数
            total_tasks = session.query(FlowHistory).filter(
                FlowHistory.claimed_by == w.id
            ).count()
            
            running_tasks = session.query(FlowHistory).filter(
                FlowHistory.claimed_by == w.id,
                FlowHistory.status == 'running'
            ).count()
            
            claimed_tasks = session.query(FlowHistory).filter(
                FlowHistory.claimed_by == w.id,
                FlowHistory.status == 'claimed'
            ).count()
            
            workers_data.append({
                'worker': w,
                'tags': json.loads(w.tags or '[]'),
                'total_tasks': total_tasks,
                'running_tasks': running_tasks,
                'claimed_tasks': claimed_tasks
            })
        
        return render_template(
            'workers/list.html',
            workers=workers_data
        )
    except Exception as e:
        return render_template(
            'workers/list.html',
            workers=[],
            error=str(e)
        )
    finally:
        session.close()


# ============ API路由 ============


@workers_bp.route('/api/workers', methods=['GET'])
def list_workers():
    """获取所有 Worker 列表
    
    Returns:
        JSON response with list of workers including:
        - id: Worker ID
        - hostname: Worker hostname
        - tags: Worker capability tags
        - status: Worker status (online/offline/draining)
        - last_heartbeat: Last heartbeat timestamp
        - registered_at: Registration timestamp
    """
    session = get_session()
    try:
        workers = session.query(Worker).all()
        result = [{
            'id': w.id,
            'hostname': w.hostname,
            'tags': json.loads(w.tags or '[]'),
            'status': w.status,
            'last_heartbeat': w.last_heartbeat.isoformat() if w.last_heartbeat else None,
            'registered_at': w.registered_at.isoformat() if w.registered_at else None
        } for w in workers]
        return jsonify({
            'success': True,
            'data': {'workers': result}
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()


@workers_bp.route('/api/workers/<worker_id>', methods=['GET'])
def get_worker(worker_id: str):
    """获取单个 Worker 详情
    
    Args:
        worker_id: Worker ID
        
    Returns:
        JSON response with worker details including:
        - Basic worker info (id, hostname, tags, status, etc.)
        - task_count: Total tasks claimed by this worker
        - running_task: Currently running task ID (if any)
    """
    session = get_session()
    try:
        worker = session.query(Worker).filter(Worker.id == worker_id).first()
        if not worker:
            return jsonify({'success': False, 'error': 'Worker not found'}), 404
        
        # 统计任务数
        task_count = session.query(FlowHistory).filter(
            FlowHistory.claimed_by == worker_id
        ).count()
        
        # 获取当前运行的任务
        running_task = session.query(FlowHistory).filter(
            FlowHistory.claimed_by == worker_id,
            FlowHistory.status == 'running'
        ).first()
        
        result = {
            'id': worker.id,
            'hostname': worker.hostname,
            'tags': json.loads(worker.tags or '[]'),
            'status': worker.status,
            'last_heartbeat': worker.last_heartbeat.isoformat() if worker.last_heartbeat else None,
            'registered_at': worker.registered_at.isoformat() if worker.registered_at else None,
            'task_count': task_count,
            'running_task': running_task.id if running_task else None
        }
        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()



@workers_bp.route('/api/workers/<worker_id>/drain', methods=['POST'])
def drain_worker(worker_id: str):
    """设置 Worker 为 draining 状态
    
    Draining 状态下，Worker 不会接受新任务，但会继续执行当前任务直到完成。
    
    Args:
        worker_id: Worker ID
        
    Returns:
        JSON response with success status
    """
    session = get_session()
    try:
        worker = session.query(Worker).filter(Worker.id == worker_id).first()
        if not worker:
            return jsonify({'success': False, 'error': 'Worker not found'}), 404
        
        if worker.status == 'offline':
            return jsonify({'success': False, 'error': 'Cannot drain an offline worker'}), 400
        
        worker.status = 'draining'
        session.commit()
        return jsonify({
            'success': True,
            'data': {'status': 'draining'},
            'message': f'Worker {worker_id} is now draining'
        })
    except Exception as e:
        session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()


@workers_bp.route('/api/workers/<worker_id>/release', methods=['POST'])
def release_worker_tasks(worker_id: str):
    """释放 Worker 的待执行任务
    
    将该 Worker 认领但尚未开始执行的任务（claimed 状态）重新放回队列，
    以便其他 Worker 可以认领执行。
    
    Args:
        worker_id: Worker ID
        
    Returns:
        JSON response with released task count
    """
    session = get_session()
    try:
        # 检查 Worker 是否存在
        worker = session.query(Worker).filter(Worker.id == worker_id).first()
        if not worker:
            return jsonify({'success': False, 'error': 'Worker not found'}), 404
        
        # 释放 claimed 状态的任务
        count = session.query(FlowHistory).filter(
            FlowHistory.claimed_by == worker_id,
            FlowHistory.status == 'claimed'
        ).update({
            'status': 'queued',
            'claimed_by': None,
            'claimed_at': None
        })
        session.commit()
        return jsonify({
            'success': True,
            'data': {'released_count': count},
            'message': f'Released {count} tasks from worker {worker_id}'
        })
    except Exception as e:
        session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()


@workers_bp.route('/api/workers/<worker_id>', methods=['DELETE'])
def remove_worker(worker_id: str):
    """删除 Worker 记录
    
    只能删除 offline 状态的 Worker。如果 Worker 仍在运行或 draining，
    需要先等待其下线。
    
    Args:
        worker_id: Worker ID
        
    Returns:
        JSON response with success status
    """
    session = get_session()
    try:
        worker = session.query(Worker).filter(Worker.id == worker_id).first()
        if not worker:
            return jsonify({'success': False, 'error': 'Worker not found'}), 404
        
        if worker.status != 'offline':
            return jsonify({
                'success': False, 
                'error': f'Can only remove offline workers. Current status: {worker.status}'
            }), 400
        
        session.delete(worker)
        session.commit()
        return jsonify({
            'success': True,
            'message': f'Worker {worker_id} has been removed'
        })
    except Exception as e:
        session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()
