/**
 * EdgeCloudX Dashboard — Alert & EV Handlers
 * ==============================================
 * Handles emergency alert rendering and EV fleet tracking updates.
 */

(function () {
    'use strict';

    let alertCount = 0;

    // ── Alert Handler ──

    window.handleAlertUpdate = function (data) {
        if (data.type !== 'emergency_corridor') return;

        alertCount++;

        const listEl = document.getElementById('alerts-list');
        const emptyEl = document.getElementById('alert-empty');
        const countEl = document.getElementById('alert-count');
        const pillEl = document.getElementById('active-alerts');

        // Hide empty state
        if (emptyEl) emptyEl.style.display = 'none';

        // Update count
        if (countEl) countEl.textContent = `${alertCount} Active`;
        if (pillEl) pillEl.textContent = alertCount;

        // Create alert item
        const alertItem = document.createElement('div');
        alertItem.className = 'alert-item';
        alertItem.innerHTML = `
            <span class="alert-icon">🚨</span>
            <div class="alert-info">
                <span class="alert-title">
                    Emergency: ${data.emergency_type || 'Unknown'}
                    (${data.emergency_id || 'N/A'})
                </span>
                <span class="alert-desc">
                    Green corridor: ${data.distance || 0} intersections,
                    ETA ${data.eta_seconds || 0}s
                </span>
            </div>
            <span class="alert-time">${new Date().toLocaleTimeString('en-US', {
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            })}</span>
        `;

        // Prepend (newest first)
        if (listEl) {
            listEl.insertBefore(alertItem, listEl.firstChild);

            // Keep max 10 alerts
            while (listEl.children.length > 11) {
                listEl.removeChild(listEl.lastChild);
            }
        }

        // Highlight affected intersections on the grid
        if (data.green_intersections) {
            data.green_intersections.forEach(iid => {
                const cell = document.getElementById(`cell-${iid}`);
                if (cell) {
                    cell.classList.add('emergency-active');
                    // Remove emergency highlight after corridor TTL
                    setTimeout(() => {
                        cell.classList.remove('emergency-active');
                    }, (data.eta_seconds || 60) * 1000);
                }
            });
        }

        // Auto-clear alert after 2 minutes
        setTimeout(() => {
            alertCount = Math.max(0, alertCount - 1);
            if (countEl) countEl.textContent = `${alertCount} Active`;
            if (pillEl) pillEl.textContent = alertCount;
            if (alertItem.parentNode) {
                alertItem.remove();
            }
            if (alertCount === 0 && emptyEl) {
                emptyEl.style.display = 'flex';
            }
        }, 120000);
    };

    // ── EV Fleet Handler ──

    window.handleEVUpdate = function (data) {
        if (data.type !== 'route_optimization') return;

        const evCountEl = document.getElementById('stat-ev-count');
        if (evCountEl) evCountEl.textContent = data.ev_count || 0;

        const updateTimeEl = document.getElementById('ev-update-time');
        if (updateTimeEl) {
            updateTimeEl.textContent = new Date().toLocaleTimeString('en-US', {
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            });
        }

        // Update individual EV route info
        if (data.routes) {
            Object.entries(data.routes).forEach(([evId, routeInfo]) => {
                const evEl = document.getElementById(evId);
                if (evEl) {
                    const posEl = evEl.querySelector('.ev-position');
                    if (posEl) {
                        posEl.textContent = `Route: ${routeInfo.steps} steps, cost: ${routeInfo.cost}`;
                    }
                }
            });
        }
    };
})();
