/**
 * Flowy管理系统 - 公共JavaScript函数
 */

// 工具函数
const Utils = {
    // 格式化日期时间
    formatDateTime: function(dateString) {
        if (!dateString) return '-';
        const date = new Date(dateString);
        return date.toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    },

    // 格式化JSON
    formatJSON: function(data) {
        try {
            const obj = typeof data === 'string' ? JSON.parse(data) : data;
            return JSON.stringify(obj, null, 2);
        } catch (e) {
            return data;
        }
    },

    // 复制到剪贴板
    copyToClipboard: function(text) {
        if (navigator.clipboard) {
            navigator.clipboard.writeText(text).then(() => {
                this.showToast('已复制到剪贴板', 'success');
            }).catch(err => {
                console.error('复制失败:', err);
                this.fallbackCopyToClipboard(text);
            });
        } else {
            this.fallbackCopyToClipboard(text);
        }
    },

    // 备用复制方法
    fallbackCopyToClipboard: function(text) {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        textArea.style.top = '-999999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();

        try {
            document.execCommand('copy');
            this.showToast('已复制到剪贴板', 'success');
        } catch (err) {
            console.error('复制失败:', err);
            this.showToast('复制失败，请手动复制', 'error');
        }

        document.body.removeChild(textArea);
    },

    // 显示提示消息
    showToast: function(message, type = 'info') {
        // 创建toast容器（如果不存在）
        let toastContainer = document.getElementById('toastContainer');
        if (!toastContainer) {
            toastContainer = document.createElement('div');
            toastContainer.id = 'toastContainer';
            toastContainer.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                z-index: 9999;
            `;
            document.body.appendChild(toastContainer);
        }

        // 创建toast元素
        const toast = document.createElement('div');
        toast.className = `alert alert-${type} alert-dismissible fade show`;
        toast.style.cssText = `
            margin-bottom: 10px;
            min-width: 300px;
            box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15);
        `;
        toast.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;

        toastContainer.appendChild(toast);

        // 自动移除
        setTimeout(() => {
            toast.remove();
        }, 5000);
    },

    // 确认对话框
    confirm: function(message, callback) {
        if (window.confirm(message)) {
            callback();
        }
    },

    // 获取URL参数
    getUrlParam: function(name) {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get(name);
    },

    // 设置URL参数
    setUrlParam: function(name, value) {
        const url = new URL(window.location);
        url.searchParams.set(name, value);
        window.history.replaceState({}, '', url);
    }
};

// API请求封装
const API = {
    // 基础请求方法
    request: function(url, options = {}) {
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json'
            }
        };

        options = { ...defaultOptions, ...options };

        return fetch(url, options)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .catch(error => {
                console.error('API请求失败:', error);
                Utils.showToast('请求失败: ' + error.message, 'error');
                throw error;
            });
    },

    // GET请求
    get: function(url) {
        return this.request(url);
    },

    // POST请求
    post: function(url, data) {
        return this.request(url, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    },

    // PUT请求
    put: function(url, data) {
        return this.request(url, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    },

    // DELETE请求
    delete: function(url) {
        return this.request(url, {
            method: 'DELETE'
        });
    }
};

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 初始化工具提示
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 自动刷新功能
    const autoRefreshElements = document.querySelectorAll('[data-auto-refresh]');
    if (autoRefreshElements.length > 0) {
        const refreshInterval = parseInt(document.body.getAttribute('data-refresh-interval') || '5000');
        setInterval(() => {
            location.reload();
        }, refreshInterval);
    }

    // 优化长文本显示
    const longTexts = document.querySelectorAll('[data-truncate]');
    longTexts.forEach(element => {
        const maxLength = parseInt(element.getAttribute('data-truncate') || '100');
        const text = element.textContent;
        if (text.length > maxLength) {
            element.textContent = text.substring(0, maxLength) + '...';
            element.title = text;
        }
    });
});

// 导出工具
window.Utils = Utils;
window.API = API;