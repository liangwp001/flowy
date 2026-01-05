#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""REST API控制器"""

from flask import Blueprint, jsonify, request
from flowy.web.services.flow_service import FlowService
from flowy.web.services.scheduler_service import SchedulerService

api_bp = Blueprint('api', __name__)


@api_bp.route('/flows', methods=['GET'])
def api_get_flows():
    """获取Flow列表API"""
    try:
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 20)), 100)
        search = request.args.get('search', '').strip()

        flows, total_pages = FlowService.get_flows_paginated(
            page=page,
            per_page=per_page,
            search=search if search else None
        )

        flows_data = []
        for flow in flows:
            stats = FlowService.get_flow_statistics(flow.id)
            flows_data.append({
                'id': flow.id,
                'name': flow.name,
                'description': flow.description,
                'created_at': flow.created_at.isoformat() if flow.created_at else None,
                'updated_at': flow.updated_at.isoformat() if flow.updated_at else None,
                'statistics': stats
            })

        return jsonify({
            'success': True,
            'data': {
                'flows': flows_data,
                'pagination': {
                    'current_page': page,
                    'total_pages': total_pages,
                    'per_page': per_page,
                    'search': search
                }
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@api_bp.route('/flows/<flow_id>', methods=['GET'])
def api_get_flow(flow_id):
    """获取单个Flow信息API"""
    try:
        flow = FlowService.get_flow_by_id(flow_id)
        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        stats = FlowService.get_flow_statistics(flow_id)

        return jsonify({
            'success': True,
            'data': {
                'id': flow.id,
                'name': flow.name,
                'description': flow.description,
                'created_at': flow.created_at.isoformat() if flow.created_at else None,
                'updated_at': flow.updated_at.isoformat() if flow.updated_at else None,
                'statistics': stats
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@api_bp.route('/flows/<flow_id>/history', methods=['GET'])
def api_get_flow_history(flow_id):
    """获取Flow执行历史API"""
    try:
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 30)), 100)
        status_filter = request.args.get('status', '')

        flow = FlowService.get_flow_by_id(flow_id)
        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        histories, total_pages = FlowService.get_flow_history_paginated(
            flow_id=flow_id,
            page=page,
            per_page=per_page,
            status_filter=status_filter if status_filter else None
        )

        histories_data = [{
            'id': h.id,
            'flow_id': h.flow_id,
            'status': h.status,
            'created_at': h.created_at.isoformat() if h.created_at else None,
            'start_time': h.start_time.isoformat() if h.start_time else None,
            'end_time': h.end_time.isoformat() if h.end_time else None,
            'input_data': h.input_data,
            'output_data': h.output_data,
            'flow_metadata': h.flow_metadata,
            'running_tasks': getattr(h, 'running_tasks', [])
        } for h in histories]

        return jsonify({
            'success': True,
            'data': {
                'histories': histories_data,
                'pagination': {
                    'current_page': page,
                    'total_pages': total_pages,
                    'per_page': per_page,
                    'status_filter': status_filter
                }
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@api_bp.route('/history/<int:history_id>', methods=['GET'])
def api_get_history_detail(history_id):
    """获取执行历史详情API"""
    try:
        detail = FlowService.get_flow_history_detail(history_id)
        if not detail:
            return jsonify({'success': False, 'error': 'History not found'}), 404

        flow_history = detail['flow_history']
        task_histories = detail['task_histories']

        tasks_data = [{
            'id': t.id,
            'flow_history_id': t.flow_history_id,
            'name': t.name,
            'status': t.status,
            'created_at': t.created_at.isoformat() if t.created_at else None,
            'start_time': t.start_time.isoformat() if t.start_time else None,
            'end_time': t.end_time.isoformat() if t.end_time else None,
            'input_data': t.input_data,
            'output_data': t.output_data,
            'progress': t.progress,
            'progress_message': t.progress_message,
            'progress_updated_at': t.progress_updated_at.isoformat() if t.progress_updated_at else None
        } for t in task_histories]

        return jsonify({
            'success': True,
            'data': {
                'flow_history': {
                    'id': flow_history.id,
                    'flow_id': flow_history.flow_id,
                    'status': flow_history.status,
                    'created_at': flow_history.created_at.isoformat() if flow_history.created_at else None,
                    'start_time': flow_history.start_time.isoformat() if flow_history.start_time else None,
                    'end_time': flow_history.end_time.isoformat() if flow_history.end_time else None,
                    'input_data': flow_history.input_data,
                    'output_data': flow_history.output_data,
                    'flow_metadata': flow_history.flow_metadata
                },
                'task_histories': tasks_data
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@api_bp.route('/flows/<flow_id>/run', methods=['POST'])
def api_run_flow(flow_id):
    """立即执行Flow API"""
    try:
        data = request.get_json() or {}
        input_data = data.get('input_data', {})
        delay_seconds = int(data.get('delay_seconds', 1))

        flow = FlowService.get_flow_by_id(flow_id)
        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        result = SchedulerService.add_immediate_job(
            flow_id=flow_id,
            input_data=input_data,
            delay_seconds=delay_seconds
        )

        return jsonify({
            'success': True,
            'data': {
                'job_id': result['job_id'],
                'flow_history_id': result['flow_history_id'],
                'message': f'Flow {flow_id} has been scheduled for execution',
                'delay_seconds': delay_seconds
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@api_bp.route('/jobs/<job_id>/status', methods=['GET'])
def api_get_job_status(job_id):
    """获取任务状态API"""
    try:
        job_status = SchedulerService.get_job_status(job_id)
        if job_status is None:
            return jsonify({'success': False, 'error': 'Job not found'}), 404

        return jsonify({
            'success': True,
            'data': job_status
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@api_bp.route('/history/<int:history_id>', methods=['DELETE'])
def api_delete_history(history_id):
    """删除单条执行历史API"""
    try:
        result = FlowService.delete_flow_history(history_id)
        if result:
            return jsonify({
                'success': True,
                'message': f'执行历史 #{history_id} 已删除'
            })
        else:
            return jsonify({'success': False, 'error': 'History not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@api_bp.route('/history/batch-delete', methods=['POST'])
def api_batch_delete_history():
    """批量删除执行历史API"""
    try:
        data = request.get_json() or {}
        history_ids = data.get('history_ids', [])

        if not history_ids:
            return jsonify({'success': False, 'error': 'No history IDs provided'}), 400

        if not isinstance(history_ids, list):
            return jsonify({'success': False, 'error': 'history_ids must be a list'}), 400

        success_count, failed_count = FlowService.batch_delete_flow_history(history_ids)

        return jsonify({
            'success': True,
            'data': {
                'success_count': success_count,
                'failed_count': failed_count,
                'total_count': len(history_ids)
            },
            'message': f'成功删除 {success_count} 条记录' +
                      (f'，失败 {failed_count} 条' if failed_count > 0 else '')
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500



