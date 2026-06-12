/**
 * EdgeCloudX Dashboard — WebSocket Connection Manager
 * =====================================================
 * Manages WebSocket connections with auto-reconnect, heartbeat,
 * and message routing to registered handlers.
 */

class EdgeCloudXWebSocket {
    constructor(path, onMessage, options = {}) {
        this.path = path;
        this.onMessage = onMessage;
        this.reconnectDelay = options.reconnectDelay || 2000;
        this.maxReconnectDelay = options.maxReconnectDelay || 30000;
        this.heartbeatInterval = options.heartbeatInterval || 15000;
        this.ws = null;
        this._reconnectAttempts = 0;
        this._heartbeatTimer = null;
        this._reconnectTimer = null;
        this._running = true;
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${protocol}//${window.location.host}/${this.path}`;

        try {
            this.ws = new WebSocket(url);
        } catch (e) {
            console.error(`[WS] Failed to create WebSocket: ${e}`);
            this._scheduleReconnect();
            return;
        }

        this.ws.onopen = () => {
            console.log(`[WS] Connected: ${this.path}`);
            this._reconnectAttempts = 0;
            this._startHeartbeat();
            this._updateStatus(true);
        };

        this.ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.type === 'pong') return;
                this.onMessage(data);
            } catch (e) {
                console.warn(`[WS] Parse error: ${e}`);
            }
        };

        this.ws.onclose = (event) => {
            console.log(`[WS] Disconnected: ${this.path} (code=${event.code})`);
            this._stopHeartbeat();
            this._updateStatus(false);
            if (this._running) {
                this._scheduleReconnect();
            }
        };

        this.ws.onerror = (error) => {
            console.error(`[WS] Error on ${this.path}:`, error);
        };
    }

    send(data) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(data));
        }
    }

    close() {
        this._running = false;
        this._stopHeartbeat();
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
        }
        if (this.ws) {
            this.ws.close();
        }
    }

    _startHeartbeat() {
        this._stopHeartbeat();
        this._heartbeatTimer = setInterval(() => {
            this.send({ type: 'ping' });
        }, this.heartbeatInterval);
    }

    _stopHeartbeat() {
        if (this._heartbeatTimer) {
            clearInterval(this._heartbeatTimer);
            this._heartbeatTimer = null;
        }
    }

    _scheduleReconnect() {
        this._reconnectAttempts++;
        const delay = Math.min(
            this.reconnectDelay * Math.pow(1.5, this._reconnectAttempts - 1),
            this.maxReconnectDelay
        );
        console.log(`[WS] Reconnecting ${this.path} in ${Math.round(delay)}ms (attempt ${this._reconnectAttempts})`);
        this._reconnectTimer = setTimeout(() => this.connect(), delay);
    }

    _updateStatus(connected) {
        const dot = document.querySelector('#ws-status .status-dot');
        const text = document.querySelector('#ws-status .status-text');
        if (dot && text) {
            if (connected) {
                dot.className = 'status-dot connected';
                text.textContent = 'Connected';
            } else {
                dot.className = 'status-dot disconnected';
                text.textContent = 'Reconnecting...';
            }
        }
    }
}

// ── Initialize WebSocket connections ──

const wsTraffic = new EdgeCloudXWebSocket('ws/traffic/', (data) => {
    if (window.handleTrafficUpdate) {
        window.handleTrafficUpdate(data);
    }
});

const wsHeatmap = new EdgeCloudXWebSocket('ws/heatmap/', (data) => {
    if (window.handleHeatmapUpdate) {
        window.handleHeatmapUpdate(data);
    }
});

const wsAlerts = new EdgeCloudXWebSocket('ws/alerts/', (data) => {
    if (window.handleAlertUpdate) {
        window.handleAlertUpdate(data);
    }
});

const wsEV = new EdgeCloudXWebSocket('ws/ev/', (data) => {
    if (window.handleEVUpdate) {
        window.handleEVUpdate(data);
    }
});

// Connect all WebSockets on page load
document.addEventListener('DOMContentLoaded', () => {
    wsTraffic.connect();
    wsHeatmap.connect();
    wsAlerts.connect();
    wsEV.connect();

    // Update clock
    function updateClock() {
        const el = document.getElementById('clock');
        if (el) {
            const now = new Date();
            el.textContent = now.toLocaleTimeString('en-US', {
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            });
        }
    }
    updateClock();
    setInterval(updateClock, 1000);
});
