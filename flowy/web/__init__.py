#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy Web模块"""

import json
import os

from flask import Flask, render_template, jsonify, request

from flowy.core.db import Flow, FlowHistory, TaskHistory, get_session


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

    # 注册模板过滤器
    @app.template_filter('datetime')
    def format_datetime(value, format='%Y-%m-%d %H:%M:%S'):
        if value is None:
            return ''
        return value.strftime(format)

    @app.template_filter('time_format')
    def format_time(value, format='%H:%M:%S'):
        if value is None:
            return ''
        return value.strftime(format)

    @app.template_filter('duration')
    def format_duration(start_time, end_time):
        if start_time is None or end_time is None:
            return '-'
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
        from datetime import datetime, timedelta
        if value is None:
            return ''

        now = datetime.now()
        if isinstance(value, str):
            value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')

        delta = now - value
        seconds = delta.total_seconds()

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

    # 主页重定向
    @app.route('/')
    def index():
        from flask import redirect, url_for
        return redirect(url_for('flows.list_flows'))

    return app
