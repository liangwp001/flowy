#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy Web模块"""

import json
import os
from functools import wraps

from flask import Flask, render_template, jsonify, request, Response

from flowy.core.db import Flow, FlowHistory, TaskHistory, get_session
from flowy.core.config import get_config


def check_auth(username: str, password: str) -> bool:
    """验证用户名和密码"""
    config = get_config()
    return username == config.auth_username and password == config.auth_password


def authenticate():
    """返回401认证响应"""
    return Response(
        '需要登录才能访问此页面',
        401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )


def requires_auth(f):
    """认证装饰器"""
    @wraps(f)
    def decorated(*args, **kwargs):
        config = get_config()
        if not config.auth_enabled:
            return f(*args, **kwargs)

        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


def create_app():
    """创建Flask应用实例"""
    # 获取模板和静态文件目录
    package_dir = os.path.dirname(__file__)

    app = Flask(
        __name__,
        template_folder=os.path.join(package_dir, 'templates'),
        static_folder=os.path.join(package_dir, 'static')
    )

    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or 'flowy-secret-key'
    app.config['FLOWS_PER_PAGE'] = 20
    app.config['HISTORY_PER_PAGE'] = 30

    # 注入站点名称到所有模板
    @app.context_processor
    def inject_site_config():
        config = get_config()
        return {
            'site_name': config.site_name
        }

    # 全局认证检查
    @app.before_request
    def before_request_auth():
        config = get_config()
        if not config.auth_enabled:
            return None

        # 静态文件不需要认证
        if request.path.startswith('/static/'):
            return None

        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()

    # 注册模板过滤器
    @app.template_filter('datetime')
    def format_datetime(value):
        """智能日期时间格式化：当年不显示年份"""
        if value is None:
            return ''
        from datetime import datetime
        current_year = datetime.now().year
        if value.year == current_year:
            # 当年：只显示月日时:分:秒
            return value.strftime('%m-%d %H:%M:%S')
        else:
            # 非当年：显示完整日期
            return value.strftime('%Y-%m-%d %H:%M:%S')

    @app.template_filter('time_format')
    def format_time(value, format='%H:%M:%S'):
        if value is None:
            return ''
        return value.strftime(format)

    @app.template_filter('duration')
    def format_duration(start_time, end_time):
        from datetime import datetime
        if start_time is None:
            return '-'
        # 如果 end_time 为 None（运行中），使用当前时间
        if end_time is None:
            end_time = datetime.now()
        duration = end_time - start_time
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f'{hours}小时{minutes}分{seconds}秒'
        elif minutes:
            return f'{minutes}分{seconds}秒'
        else:
            return f'{seconds}秒'

    @app.template_filter('time_ago')
    def format_time_ago(value):
        """格式化时间为相对时间（如：2小时前）"""
        from datetime import datetime, timedelta, timezone
        if value is None:
            return ''

        # 处理时区问题
        if isinstance(value, str):
            value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        
        # 如果 value 有时区信息，转换为本地时间
        if value.tzinfo is not None:
            # 转换为本地时间（移除时区信息）
            value = value.replace(tzinfo=None)
        
        now = datetime.now()
        delta = now - value
        seconds = delta.total_seconds()

        # 如果是未来时间（下次执行时间）
        if seconds < 0:
            seconds = abs(seconds)
            if seconds < 60:
                return '即将执行'
            elif seconds < 3600:
                minutes = int(seconds / 60)
                return f'{minutes}分钟后'
            elif seconds < 86400:
                hours = int(seconds / 3600)
                return f'{hours}小时后'
            elif seconds < 604800:
                days = int(seconds / 86400)
                return f'{days}天后'
            else:
                return value.strftime('%Y-%m-%d')
        
        # 过去时间
        if seconds < 60:
            return '刚刚'
        elif seconds < 3600:
            minutes = int(seconds / 60)
            return f'{minutes}分钟前'
        elif seconds < 86400:
            hours = int(seconds / 3600)
            return f'{hours}小时前'
        elif seconds < 604800:
            days = int(seconds / 86400)
            return f'{days}天前'
        else:
            return value.strftime('%Y-%m-%d')

    @app.template_filter('time_diff')
    def format_time_diff(value):
        """格式化时间差（如：2小时前）"""
        from datetime import datetime
        if value is None:
            return '-'

        if isinstance(value, str):
            value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')

        now = datetime.now()
        diff = now - value
        seconds = int(diff.total_seconds())

        if seconds < 60:
            return '刚刚'
        elif seconds < 3600:
            minutes = seconds // 60
            return f'{minutes}分钟前'
        elif seconds < 86400:
            hours = seconds // 3600
            return f'{hours}小时前'
        else:
            days = seconds // 86400
            return f'{days}天前'

    @app.template_filter('json_format')
    def json_format(value):
        if value is None:
            return 'null'
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        return json.dumps(value, indent=2, ensure_ascii=False)

    @app.template_filter('truncate_json')
    def truncate_json(value, max_length=100):
        if value is None:
            return ''
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                formatted = json.dumps(parsed, ensure_ascii=False, separators=(',', ':'))
            except (json.JSONDecodeError, TypeError):
                formatted = value
        else:
            formatted = json.dumps(value, ensure_ascii=False, separators=(',', ':'))

        if len(formatted) <= max_length:
            return formatted
        return formatted[:max_length] + '...'

    @app.template_filter('to_json_obj')
    def to_json_obj(value):
        if value is None:
            return 'null'
        if isinstance(value, bytes):
            # 处理字节串类型
            try:
                value = value.decode('utf-8')
                value = json.loads(value)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                return json.dumps(str(value), ensure_ascii=False)
        elif isinstance(value, str):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return json.dumps(value, ensure_ascii=False)
        return json.dumps(value, ensure_ascii=False)

    # 错误处理
    @app.errorhandler(404)
    def not_found_error(error):
        if request.accept_mimetypes.accept_json and not request.accept_mimetypes.accept_html:
            return jsonify({'error': 'Not found'}), 404
        return render_template('errors/404.html'), 404

    @app.errorhandler(500)
    def internal_error(error):
        app.logger.error(f'Server Error: {error}')
        if request.accept_mimetypes.accept_json and not request.accept_mimetypes.accept_html:
            return jsonify({'error': 'Internal server error'}), 500
        return render_template('errors/500.html'), 500

    # 注册蓝图
    from flowy.web.controllers import flows_bp, api_bp, triggers_bp
    app.register_blueprint(flows_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(triggers_bp)

    # 注册应用关闭时的清理函数
    import atexit
    from flowy.core.logger import cleanup_all_flow_loggers
    atexit.register(cleanup_all_flow_loggers)

    # 主页重定向
    @app.route('/')
    def index():
        from flask import redirect, url_for
        return redirect(url_for('flows.list_flows'))

    return app
