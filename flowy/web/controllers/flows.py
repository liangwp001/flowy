#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flow页面控制器"""

from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app, jsonify, make_response
from flowy.web.services.flow_service import FlowService
from flowy.core.db import get_session, FlowHistory
from datetime import datetime

flows_bp = Blueprint('flows', __name__)


@flows_bp.route('/flows')
@flows_bp.route('/flows/<int:page>')
def list_flows(page=1):
    """Flow列表页面"""
    search = request.args.get('search', '').strip()

    try:
        flows, total_pages = FlowService.get_flows_paginated(
            page=page,
            per_page=current_app.config.get('FLOWS_PER_PAGE', 20),
            search=search if search else None
        )

        flows_with_stats = []
        for flow in flows:
            stats = FlowService.get_flow_statistics(flow.id)
            flows_with_stats.append({
                'flow': flow,
                'stats': stats
            })

        return render_template(
            'flows/list.html',
            flows=flows_with_stats,
            current_page=page,
            total_pages=total_pages,
            search=search
        )
    except Exception as e:
        flash(f'加载Flow列表失败: {str(e)}', 'error')
        return render_template(
            'flows/list.html',
            flows=[],
            current_page=1,
            total_pages=1,
            search=search
        )


@flows_bp.route('/flows/<flow_id>')
def flow_detail(flow_id):
    """Flow详情页面"""
    try:
        flow = FlowService.get_flow_by_id(flow_id)
        if not flow:
            flash('Flow不存在', 'error')
            return redirect(url_for('flows.list_flows'))

        stats = FlowService.get_flow_statistics(flow_id)

        return render_template(
            'flows/detail.html',
            flow=flow,
            stats=stats
        )
    except Exception as e:
        flash(f'加载Flow详情失败: {str(e)}', 'error')
        return redirect(url_for('flows.list_flows'))


@flows_bp.route('/flows/<flow_id>/history')
@flows_bp.route('/flows/<flow_id>/history/<int:page>')
def flow_history(flow_id, page=1):
    """Flow执行历史页面"""
    status_filter = request.args.get('status', '')

    try:
        flow = FlowService.get_flow_by_id(flow_id)
        if not flow:
            flash('Flow不存在', 'error')
            return redirect(url_for('flows.list_flows'))

        histories, total_pages = FlowService.get_flow_history_paginated(
            flow_id=flow_id,
            page=page,
            per_page=current_app.config.get('HISTORY_PER_PAGE', 30),
            status_filter=status_filter if status_filter else None
        )

        stats = FlowService.get_flow_statistics(flow_id)

        return render_template(
            'history/list.html',
            flow=flow,
            histories=histories,
            current_page=page,
            total_pages=total_pages,
            status_filter=status_filter,
            stats=stats,
            total_count=stats.get('total_count', 0)  # 添加 total_count 参数
        )
    except Exception as e:
        flash(f'加载执行历史失败: {str(e)}', 'error')
        return redirect(url_for('flows.list_flows'))


@flows_bp.route('/history/detail/<int:history_id>')
def history_detail(history_id):
    """执行历史详情页面"""
    try:
        detail = FlowService.get_flow_history_detail(history_id)
        if not detail:
            flash('执行历史不存在', 'error')
            return redirect(url_for('flows.list_flows'))

        return render_template(
            'history/detail.html',
            flow_history=detail['flow_history'],
            task_histories=detail['task_histories'],
            flow=detail['flow']
        )
    except Exception as e:
        flash(f'加载历史详情失败: {str(e)}', 'error')
        return redirect(url_for('flows.list_flows'))


@flows_bp.route('/api/history/<int:history_id>/logs')
def api_get_history_logs(history_id):
    """获取执行历史的日志内容"""
    try:
        from flowy.core.config import get_config
        import os

        config = get_config()
        log_file = os.path.join(config.log_dir, f'flow-history-{history_id}.log')

        if not os.path.exists(log_file):
            return jsonify({
                'success': False,
                'error': '日志文件不存在'
            }), 404

        # 读取日志文件内容
        with open(log_file, 'r', encoding='utf-8') as f:
            log_content = f.read()

        return jsonify({
            'success': True,
            'data': {
                'log_content': log_content,
                'log_file': log_file
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500





# API端点
@flows_bp.route('/api/running-count')
def api_running_count():
    """获取正在运行的任务数量"""
    try:
        session = get_session()
        running_count = session.query(FlowHistory).filter(FlowHistory.status == 'running').count()
        session.close()
        return jsonify({'count': running_count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500





@flows_bp.route('/api/flows/<flow_id>/chart-data')
def api_flow_chart_data(flow_id):
    """获取Flow图表数据"""
    try:
        days = request.args.get('days', 30, type=int)
        chart_data = FlowService.get_flow_chart_data(flow_id, days)
        return jsonify({
            'success': True,
            'data': chart_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@flows_bp.route('/api/flows/<flow_id>/history')
def api_flow_history(flow_id):
    """获取Flow执行历史API"""
    try:
        per_page = request.args.get('per_page', 10, type=int)
        page = request.args.get('page', 1, type=int)
        status_filter = request.args.get('status')

        histories, total_pages = FlowService.get_flow_history_paginated(
            flow_id=flow_id,
            page=page,
            per_page=per_page,
            status_filter=status_filter
        )

        # 转换为字典格式
        histories_data = []
        for h in histories:
            histories_data.append({
                'id': h.id,
                'status': h.status,
                'created_at': h.created_at.isoformat() if h.created_at else None,
                'start_time': h.start_time.isoformat() if h.start_time else None,
                'end_time': h.end_time.isoformat() if h.end_time else None,
                'flow_name': flow_id
            })

        return jsonify({
            'success': True,
            'data': {
                'histories': histories_data,
                'total_pages': total_pages,
                'current_page': page
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


