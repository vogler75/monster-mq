// Kafka Topics Monitoring Page Controller

class KafkaTopicMonitor {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.servers = [];
        this.selectedServer = null;
        this.selectedTopic = null;
        this.loadedMessages = [];
        this.earliestOffset = 0;
        this.latestOffset = 0;
        
        this.init();
    }

    async init() {
        this.bindEvents();
        await this.loadServers();
    }

    bindEvents() {
        const serverSelect = document.getElementById('server-select');
        const topicSelect = document.getElementById('topic-select');
        const startOffsetSelect = document.getElementById('start-offset-select');
        const customOffsetInput = document.getElementById('custom-offset-input');
        const btnFetch = document.getElementById('btn-fetch');
        const searchInput = document.getElementById('search-input');
        const detailCloseBtn = document.getElementById('detail-close-btn');

        serverSelect.addEventListener('change', (e) => this.handleServerChange(e.target.value));
        topicSelect.addEventListener('change', (e) => this.handleTopicChange(e.target.value));
        
        startOffsetSelect.addEventListener('change', (e) => {
            if (e.target.value === 'custom') {
                customOffsetInput.style.display = 'inline-block';
            } else {
                customOffsetInput.style.display = 'none';
            }
        });

        btnFetch.addEventListener('click', () => this.fetchMessages());
        
        searchInput.addEventListener('input', (e) => this.filterMessages(e.target.value));
        
        detailCloseBtn.addEventListener('click', () => this.closeDetailPanel());
        
        // Close detail panel when clicking outside of it (on a row or background)
        document.addEventListener('click', (e) => {
            const panel = document.getElementById('detail-panel');
            if (panel.classList.contains('open') && 
                !panel.contains(e.target) && 
                !e.target.closest('#messages-table-body tr') &&
                !e.target.closest('#detail-close-btn')) {
                this.closeDetailPanel();
            }
        });
    }

    async loadServers() {
        this.showLoading(true);
        try {
            const query = `
                query GetKafkaServers {
                    kafkaServers { 
                        name
                        enabled
                        streams {
                            streamName
                            topicFilter
                        }
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.kafkaServers) throw new Error('Invalid response structure');
            
            this.servers = result.kafkaServers.filter(s => s.enabled);
            this.populateServersDropdown();
        } catch (e) {
            console.error('Error loading Kafka servers:', e);
            this.showToast('Failed to load Kafka servers: ' + e.message, 'error');
        } finally {
            this.showLoading(false);
        }
    }

    populateServersDropdown() {
        const select = document.getElementById('server-select');
        select.innerHTML = '<option value="">Select server...</option>';
        this.servers.forEach(server => {
            const opt = document.createElement('option');
            opt.value = server.name;
            opt.textContent = server.name;
            select.appendChild(opt);
        });
    }

    handleServerChange(serverName) {
        const topicSelect = document.getElementById('topic-select');
        this.selectedServer = this.servers.find(s => s.name === serverName);
        this.closeDetailPanel();
        
        if (!this.selectedServer) {
            topicSelect.innerHTML = '<option value="">Select topic...</option>';
            topicSelect.disabled = true;
            document.getElementById('stats-banner').style.display = 'none';
            return;
        }

        topicSelect.innerHTML = '<option value="">Select topic...</option>';
        if (this.selectedServer.streams && this.selectedServer.streams.length > 0) {
            this.selectedServer.streams.forEach(stream => {
                const opt = document.createElement('option');
                opt.value = stream.streamName;
                opt.textContent = stream.streamName + ' (' + stream.topicFilter + ')';
                topicSelect.appendChild(opt);
            });
            topicSelect.disabled = false;
        } else {
            const opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'No streams configured';
            topicSelect.appendChild(opt);
            topicSelect.disabled = true;
        }
        document.getElementById('stats-banner').style.display = 'none';
    }

    async handleTopicChange(topicName) {
        this.selectedTopic = topicName;
        this.closeDetailPanel();
        if (!this.selectedTopic) {
            document.getElementById('stats-banner').style.display = 'none';
            return;
        }

        await this.loadTopicOffsets();
    }

    async loadTopicOffsets() {
        try {
            const query = `
                query GetKafkaTopicOffsets($serverName: String!, $topic: String!) {
                    kafkaTopicOffsets(serverName: $serverName, topic: $topic) {
                        earliestOffset
                        latestOffset
                    }
                }
            `;
            const vars = {
                serverName: this.selectedServer.name,
                topic: this.selectedTopic
            };
            const result = await this.client.query(query, vars);
            if (result && result.kafkaTopicOffsets) {
                const offsets = result.kafkaTopicOffsets;
                this.earliestOffset = offsets.earliestOffset;
                this.latestOffset = offsets.latestOffset;
                
                const count = Math.max(0, this.latestOffset - this.earliestOffset);
                
                document.getElementById('stat-earliest').textContent = this.earliestOffset;
                document.getElementById('stat-latest').textContent = this.latestOffset;
                document.getElementById('stat-count').textContent = count;
                document.getElementById('stats-banner').style.display = 'flex';
            }
        } catch (e) {
            console.error('Error fetching topic offsets:', e);
        }
    }

    async fetchMessages() {
        if (!this.selectedServer || !this.selectedTopic) {
            this.showToast('Please select a server and a topic.', 'warning');
            return;
        }

        this.showLoading(true);
        this.showMessagesLoading(true);
        this.closeDetailPanel();
        
        const startOffsetSelect = document.getElementById('start-offset-select').value;
        const customOffsetInput = document.getElementById('custom-offset-input').value;
        const limitSelect = parseInt(document.getElementById('limit-select').value) || 100;
        
        let startOffset = null;
        let limit = limitSelect;
        
        if (startOffsetSelect === 'earliest') {
            startOffset = this.earliestOffset;
        } else if (startOffsetSelect === 'custom') {
            if (customOffsetInput === '') {
                this.showToast('Please enter a custom start offset.', 'warning');
                this.showLoading(false);
                return;
            }
            startOffset = parseInt(customOffsetInput) || 0;
        } else if (startOffsetSelect === 'newest-100') {
            limit = 100;
            startOffset = null; // Let backend resolve to latest 100
        } else if (startOffsetSelect === 'newest-500') {
            limit = 500;
            startOffset = null; // Let backend resolve to latest 500
        }

        try {
            const query = `
                query GetKafkaMessages($serverName: String!, $topic: String!, $startOffset: Long, $limit: Int) {
                    kafkaMessages(serverName: $serverName, topic: $topic, startOffset: $startOffset, limit: $limit) {
                        offset
                        topic
                        partition
                        timestamp
                        key
                        value
                        size
                    }
                }
            `;
            const vars = {
                serverName: this.selectedServer.name,
                topic: this.selectedTopic,
                startOffset: startOffset,
                limit: limit
            };
            const result = await this.client.query(query, vars);
            if (!result || !result.kafkaMessages) throw new Error('Invalid response structure');
            
            this.loadedMessages = result.kafkaMessages;
            // Redpanda lists newest messages at the top, so we reverse the order
            this.loadedMessages.reverse();
            
            this.renderMessagesTable();
            await this.loadTopicOffsets(); // Refresh stats banner too
        } catch (e) {
            console.error('Error fetching Kafka messages:', e);
            this.showToast('Failed to fetch messages: ' + e.message, 'error');
            const tbody = document.getElementById('messages-table-body');
            if (tbody) {
                tbody.innerHTML = `<tr><td colspan="4" class="empty-state">Failed to load messages. Please retry.</td></tr>`;
            }
        } finally {
            this.showLoading(false);
        }
    }

    renderMessagesTable() {
        const tbody = document.getElementById('messages-table-body');
        const countBar = document.getElementById('result-count-bar');
        
        tbody.innerHTML = '';
        if (this.loadedMessages.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" class="empty-state">No messages found in this range.</td></tr>`;
            countBar.style.display = 'none';
            return;
        }

        this.loadedMessages.forEach((msg, idx) => {
            const tr = document.createElement('tr');
            tr.dataset.index = idx;
            tr.addEventListener('click', () => this.selectMessage(idx, tr));

            // Format timestamp nicely
            let formattedTime = msg.timestamp;
            try {
                const date = new Date(msg.timestamp);
                if (!isNaN(date.getTime())) {
                    formattedTime = date.toLocaleString();
                }
            } catch (e) {}

            // Truncate and clean up value preview
            let valuePreview = msg.value || '';
            if (valuePreview.length > 150) {
                valuePreview = valuePreview.substring(0, 150) + '...';
            }

            tr.innerHTML = `
                <td class="offset-cell">${msg.offset}</td>
                <td class="timestamp-cell">${this.escapeHtml(formattedTime)}</td>
                <td class="key-cell">${this.escapeHtml(msg.key || '')}</td>
                <td class="payload-cell">${this.escapeHtml(valuePreview)}</td>
            `;
            tbody.appendChild(tr);
        });

        document.getElementById('displayed-count').textContent = this.loadedMessages.length;
        document.getElementById('total-count').textContent = this.loadedMessages.length;
        countBar.style.display = 'block';
    }

    filterMessages(filterText) {
        const tbody = document.getElementById('messages-table-body');
        const rows = tbody.querySelectorAll('tr');
        if (rows.length === 0 || this.loadedMessages.length === 0) return;

        let visibleCount = 0;
        const query = filterText.toLowerCase().trim();

        this.loadedMessages.forEach((msg, idx) => {
            const tr = tbody.querySelector(`tr[data-index="${idx}"]`);
            if (!tr) return;

            const matches = 
                msg.offset.toString().includes(query) ||
                (msg.key && msg.key.toLowerCase().includes(query)) ||
                (msg.value && msg.value.toLowerCase().includes(query)) ||
                msg.timestamp.toLowerCase().includes(query);

            if (matches) {
                tr.style.display = '';
                visibleCount++;
            } else {
                tr.style.display = 'none';
            }
        });

        document.getElementById('displayed-count').textContent = visibleCount;
    }

    selectMessage(idx, rowElement) {
        const tbody = document.getElementById('messages-table-body');
        tbody.querySelectorAll('tr').forEach(r => r.classList.remove('selected'));
        rowElement.classList.add('selected');

        const msg = this.loadedMessages[idx];
        
        let formattedTime = msg.timestamp;
        try {
            const date = new Date(msg.timestamp);
            if (!isNaN(date.getTime())) {
                formattedTime = date.toLocaleString();
            }
        } catch (e) {}

        document.getElementById('detail-offset').textContent = msg.offset;
        document.getElementById('detail-partition').textContent = msg.partition;
        document.getElementById('detail-timestamp').textContent = formattedTime;
        document.getElementById('detail-key').textContent = msg.key || 'null';
        document.getElementById('detail-size').textContent = msg.size + ' Bytes';

        const payloadContainer = document.getElementById('detail-payload');
        
        // Check if value is JSON
        try {
            const jsonObj = JSON.parse(msg.value);
            payloadContainer.innerHTML = `<pre style="margin:0; color:var(--monster-green); font-weight:500;">${this.escapeHtml(JSON.stringify(jsonObj, null, 2))}</pre>`;
        } catch (e) {
            // Raw text fallback
            payloadContainer.textContent = msg.value || '';
        }

        document.getElementById('detail-panel').classList.add('open');
    }

    closeDetailPanel() {
        document.getElementById('detail-panel').classList.remove('open');
        const tbody = document.getElementById('messages-table-body');
        if (tbody) {
            tbody.querySelectorAll('tr').forEach(r => r.classList.remove('selected'));
        }
    }

    showLoading(show) {
        const btn = document.getElementById('btn-fetch');
        if (btn) btn.disabled = show;
    }

    showMessagesLoading(show) {
        const tbody = document.getElementById('messages-table-body');
        if (show && tbody) {
            tbody.innerHTML = `
                <tr class="loading-row">
                    <td colspan="4">
                        <div style="display:flex; justify-content:center; align-items:center; gap:0.5rem;">
                            <span style="font-size:1.5rem; animation: spin 1s linear infinite;">⏳</span>
                            <span>Loading messages...</span>
                        </div>
                    </td>
                </tr>
            `;
        }
    }

    showToast(message, type = 'success') {
        const existing = document.getElementById('toast-notification');
        if (existing) existing.remove();

        const toast = document.createElement('div');
        toast.id = 'toast-notification';
        
        let bg = 'var(--monster-purple, #7C3AED)';
        if (type === 'error') bg = 'var(--monster-red, #EF4444)';
        else if (type === 'warning') bg = 'var(--monster-orange, #F59E0B)';

        toast.style.cssText = `
            position: fixed;
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            background: ${bg};
            color: #fff;
            padding: 14px 24px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.4);
            z-index: 10000;
            font-size: 0.9rem;
            max-width: 600px;
            display: flex;
            align-items: center;
            gap: 10px;
            animation: slideDown 0.3s ease-out;
        `;
        
        toast.innerHTML = `
            <span style="font-size:1.2rem;">${type === 'error' ? '⚠️' : 'ℹ️'}</span>
            <span>${this.escapeHtml(message)}</span>
            <button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>
        `;

        if (!document.getElementById('toast-anim-style')) {
            const s = document.createElement('style');
            s.id = 'toast-anim-style';
            s.textContent = `
                @keyframes slideDown { from { transform: translateX(-50%) translateY(-100%); opacity:0; } to { transform: translateX(-50%) translateY(0); opacity:1; } }
                @keyframes fadeOut { from { opacity:1; } to { opacity:0; } }
                @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
            `;
            document.head.appendChild(s);
        }

        document.body.appendChild(toast);
        setTimeout(() => {
            if (toast.parentElement) {
                toast.style.animation = 'fadeOut 0.3s ease-out forwards';
                setTimeout(() => { if (toast.parentElement) toast.remove(); }, 300);
            }
        }, 4000);
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Instantiate
var kafkaTopicMonitor;
document.addEventListener('DOMContentLoaded', () => {
    kafkaTopicMonitor = new KafkaTopicMonitor();
});
