#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy上下文管理模块"""

from contextvars import ContextVar

flow_history_id_var: ContextVar[int] = ContextVar('flow_history_id', default=None)


def get_flow_history_id():
    """获取当前上下文的 flow_history_id"""
    return flow_history_id_var.get()
