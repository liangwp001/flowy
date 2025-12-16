#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy Web控制器"""

from flowy.web.controllers.flows import flows_bp
from flowy.web.controllers.api import api_bp
from flowy.web.controllers.triggers import triggers_bp

__all__ = ['flows_bp', 'api_bp', 'triggers_bp']
