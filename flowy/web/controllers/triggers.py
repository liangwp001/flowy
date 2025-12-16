#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""触发器控制器"""

import logging
from flask import Blueprint, jsonify, request
from flowy.core.json_utils import json

from flowy.web.services.trigger_service import TriggerService

logger = logging.getLogger(__name__)

triggers_bp = Blueprint('triggers', __name__)


def trigger_to_dict(trigger):
    """将触发器对象转换为字典
    
    Args:
        trigger: 触发器对象
        
    Returns:
        触发器字典表示
    """
    trigger_params = {}
    if trigger.trigger_params:
        try:
            trigger_params = json.loads(trigger.trigger_params)
        except Exception:
            trigger_params = {}
    
    return {
        'id': trigger.id,
        'flow_id': trigger.flow_id,
        'name': trigger.name,
        'description': trigger.description,
        'cron_expression': trigger.cron_expression,
        'trigger_params': trigger_params,
        'enabled': bool(trigger.enabled),
        'created_at': trigger.created_at.isoformat() if trigger.created_at else None,
        'updated_at': trigger.updated_at.isoformat() if trigger.updated_at else None
    }



@triggers_bp.route('/api/flows/<flow_id>/triggers', methods=['GET'])
def api_list_triggers(flow_id):
    """获取工作流的触发器列表API
    
    Args:
        flow_id: 工作流ID
        
    Returns:
        JSON响应，包含触发器列表
    """
    try:
        triggers = TriggerService.get_triggers_by_flow(flow_id)
        return jsonify({
            'success': True,
            'data': [trigger_to_dict(t) for t in triggers]
        })
    except Exception as e:
        logger.error(f"获取触发器列表失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@triggers_bp.route('/api/triggers/<int:trigger_id>', methods=['GET'])
def api_get_trigger(trigger_id):
    """获取单个触发器API
    
    Args:
        trigger_id: 触发器ID
        
    Returns:
        JSON响应，包含触发器信息
    """
    try:
        trigger = TriggerService.get_trigger_by_id(trigger_id)
        if not trigger:
            return jsonify({'success': False, 'error': '触发器不存在'}), 404
        
        return jsonify({
            'success': True,
            'data': trigger_to_dict(trigger)
        })
    except Exception as e:
        logger.error(f"获取触发器失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@triggers_bp.route('/api/triggers', methods=['POST'])
def api_create_trigger():
    """创建触发器API
    
    Returns:
        JSON响应，包含创建的触发器信息
    """
    try:
        data = request.get_json()
        
        # 验证必填字段
        if not data:
            return jsonify({'success': False, 'error': '请求数据不能为空'}), 400
        
        required_fields = ['flow_id', 'name', 'cron_expression']
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'error': f'缺少必填字段: {field}'}), 400
        
        # 创建触发器
        trigger = TriggerService.create_trigger(
            flow_id=data['flow_id'],
            name=data['name'],
            description=data.get('description', ''),
            cron_expression=data['cron_expression'],
            trigger_params=data.get('trigger_params', {})
        )
        
        return jsonify({
            'success': True,
            'data': trigger_to_dict(trigger)
        })
    except ValueError as e:
        logger.warning(f"创建触发器验证失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"创建触发器失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@triggers_bp.route('/api/triggers/<int:trigger_id>', methods=['PUT'])
def api_update_trigger(trigger_id):
    """更新触发器API
    
    Args:
        trigger_id: 触发器ID
        
    Returns:
        JSON响应，包含更新后的触发器信息
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': '请求数据不能为空'}), 400
        
        # 更新触发器
        trigger = TriggerService.update_trigger(
            trigger_id=trigger_id,
            name=data.get('name'),
            description=data.get('description'),
            cron_expression=data.get('cron_expression'),
            trigger_params=data.get('trigger_params')
        )
        
        return jsonify({
            'success': True,
            'data': trigger_to_dict(trigger)
        })
    except ValueError as e:
        logger.warning(f"更新触发器验证失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"更新触发器失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@triggers_bp.route('/api/triggers/<int:trigger_id>', methods=['DELETE'])
def api_delete_trigger(trigger_id):
    """删除触发器API
    
    Args:
        trigger_id: 触发器ID
        
    Returns:
        JSON响应，表示删除成功
    """
    try:
        TriggerService.delete_trigger(trigger_id)
        return jsonify({'success': True})
    except ValueError as e:
        logger.warning(f"删除触发器验证失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"删除触发器失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@triggers_bp.route('/api/triggers/<int:trigger_id>/toggle', methods=['POST'])
def api_toggle_trigger(trigger_id):
    """切换触发器启用状态API
    
    Args:
        trigger_id: 触发器ID
        
    Returns:
        JSON响应，表示切换成功
    """
    try:
        data = request.get_json() or {}
        enabled = data.get('enabled', True)
        
        TriggerService.toggle_trigger_status(trigger_id, enabled)
        return jsonify({'success': True})
    except ValueError as e:
        logger.warning(f"切换触发器状态验证失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"切换触发器状态失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
