class JSONViewer {
    constructor(options = {}) {
        this.options = Object.assign({
            rootCollapsable: true,
            clickableUrls: true,
            bigNumbers: false
        }, options);
    }

    isCollapsable(arg) {
        return arg instanceof Object && Object.keys(arg).length > 0;
    }

    isUrl(string) {
        const protocols = ['http', 'https', 'ftp', 'ftps'];
        return protocols.some(protocol => string.startsWith(protocol + '://'));
    }

    htmlEscape(s) {
        return s.replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/'/g, '&apos;')
                .replace(/"/g, '&quot;');
    }

    json2html(json) {
        let html = '';
        if (typeof json === 'string') {
            json = this.htmlEscape(json);
            if (this.options.clickableUrls && this.isUrl(json)) {
                html += `<a href="${json}" class="json-value" target="_blank">${json}</a>`;
            } else {
                json = json.replace(/&quot;/g, '\\&quot;');
                html += `<span class="json-value">"${json}"</span>`;
            }
        } else if (typeof json === 'number' || typeof json === 'bigint') {
            html += `<span class="json-literal">${json}</span>`;
        } else if (typeof json === 'boolean') {
            html += `<span class="json-literal">${json}</span>`;
        } else if (json === null) {
            html += '<span class="json-literal">null</span>';
        } else if (Array.isArray(json)) {
            if (json.length > 0) {
                html += '[<ol class="json-array">';
                for (let i = 0; i < json.length; ++i) {
                    html += '<li>';
                    if (this.isCollapsable(json[i])) {
                        html += '<a class="json-toggle"></a>';
                    }
                    html += this.json2html(json[i]);
                    if (i < json.length - 1) {
                        html += ',';
                    }
                    html += '</li>';
                }
                html += '</ol>]';
            } else {
                html += '[]';
            }
        } else if (typeof json === 'object') {
            if (this.options.bigNumbers && (typeof json.toExponential === 'function' || json.isLosslessNumber)) {
                html += `<span class="json-literal">${json.toString()}</span>`;
            } else {
                const keyCount = Object.keys(json).length;
                if (keyCount > 0) {
                    html += '{<ul class="json-dict">';
                    let count = 0;
                    for (const key in json) {
                        if (Object.prototype.hasOwnProperty.call(json, key)) {
                            const jsonElement = json[key];
                            const escapedKey = this.htmlEscape(key);
                            const keyRepr = `<span class="json-key">"${escapedKey}"</span>`;
                            html += '<li>';
                            if (this.isCollapsable(jsonElement)) {
                                html += `<a class="json-toggle">${keyRepr}</a>`;
                            } else {
                                html += keyRepr;
                            }
                            html += ': ' + this.json2html(jsonElement);
                            if (++count < keyCount) {
                                html += ',';
                            }
                            html += '</li>';
                        }
                    }
                    html += '</ul>}';
                } else {
                    html += '{}';
                }
            }
        }
        return html;
    }

    render(element, jsonData) {
        const html = this.json2html(jsonData);

        if (this.options.rootCollapsable && this.isCollapsable(jsonData)) {
            element.innerHTML = '<a class="json-toggle"></a>' + html;
        } else {
            element.innerHTML = html;
        }

        const toggleChildren = (element, collapse) => {
            const childToggles = element.querySelectorAll('.json-toggle');
            childToggles.forEach(toggle => {
                const container = toggle.nextElementSibling;
                if (container && (container.classList.contains('json-dict') || container.classList.contains('json-array'))) {
                    const isCollapsed = toggle.classList.contains('collapsed');
                    if (collapse !== isCollapsed) {
                        toggle.click();
                    }
                }
            });
        };

        const handleClick = (e) => {
            if (e.target.matches('.json-toggle')) {
                e.preventDefault();
                const target = e.target;
                target.classList.toggle('collapsed');
                const container = target.nextElementSibling;
                if (container && (container.classList.contains('json-dict') || container.classList.contains('json-array'))) {
                    container.classList.toggle('hidden');
                    if (e.ctrlKey) {
                        toggleChildren(container, target.classList.contains('collapsed'));
                    }

                    const siblings = container.parentNode.children;
                    for (let i = Array.from(siblings).indexOf(container) + 1; i < siblings.length; i++) {
                        if (siblings[i].classList.contains('json-placeholder')) {
                            siblings[i].remove();
                            i--;
                        }
                    }

                    if (container.classList.contains('hidden')) {
                        const count = container.children.length;
                        let placeholder = document.createElement('a');
                        placeholder.className = 'json-placeholder';
                        placeholder.textContent = `${count} ${count > 1 ? 'items' : 'item'}`;
                        if (!container.nextElementSibling?.classList.contains('json-placeholder')) {
                            container.parentNode.insertBefore(placeholder, container.nextSibling);
                        }
                    }
                }
            } else if (e.target.matches('.json-placeholder')) {
                e.preventDefault();
                const toggle = e.target.previousElementSibling.previousElementSibling;
                if (toggle) {
                    toggle.click();
                }
            }
        };

        element.removeEventListener('click', handleClick);
        element.addEventListener('click', handleClick);
    }
}