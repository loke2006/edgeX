/**
 * EdgeCloudX Dashboard — Live Traffic Grid Renderer
 * ====================================================
 * Updates intersection cells with real-time data from WebSocket.
 */

(function () {
    'use strict';

    let totalVehicles = 0;
    let avgCongestion = 0;
    let hotspotCount = 0;

    function getCongestionLevel(score) {
        if (score < 0.25) return 'low';
        if (score < 0.50) return 'moderate';
        if (score < 0.75) return 'high';
        return 'critical';
    }

    function getCongestionColor(score) {
        if (score < 0.25) return '#34d399';
        if (score < 0.50) return '#fbbf24';
        if (score < 0.75) return '#fb923c';
        return '#f87171';
    }

    function updateIntersectionCell(data) {
        const cellId = `cell-${data.intersection_id}`;
        const cell = document.getElementById(cellId);
        if (!cell) return;

        // Update signal light
        const signal = cell.querySelector('.signal-light');
        if (signal) {
            signal.className = `signal-light ${data.signal_state || 'red'}`;
        }

        // Update vehicle count
        const countEl = cell.querySelector('.vehicle-count');
        if (countEl) {
            countEl.textContent = data.vehicle_count || '0';
        }

        // Update congestion bar
        const score = parseFloat(data.congestion_score || 0);
        const fillEl = cell.querySelector('.congestion-fill');
        if (fillEl) {
            const pct = Math.round(score * 100);
            fillEl.style.width = `${pct}%`;
            fillEl.style.background = `linear-gradient(90deg, ${getCongestionColor(score * 0.5)}, ${getCongestionColor(score)})`;
        }

        // Update data attributes
        const level = data.congestion_level || getCongestionLevel(score);
        cell.setAttribute('data-congestion', score);
        cell.setAttribute('data-congestion-level', level);

        // Emergency highlight
        if (data.is_emergency_active === 'True' || data.is_emergency_active === true) {
            cell.classList.add('emergency-active');
        } else {
            cell.classList.remove('emergency-active');
        }

        // Animate update
        cell.classList.add('cell-update');
        setTimeout(() => cell.classList.remove('cell-update'), 300);
    }

    function updateGlobalStats() {
        // Total vehicles
        const totalEl = document.getElementById('stat-total-vehicles');
        const pillEl = document.getElementById('total-vehicles');
        if (totalEl) totalEl.textContent = totalVehicles.toLocaleString();
        if (pillEl) pillEl.textContent = totalVehicles.toLocaleString();

        // Avg congestion
        const avgEl = document.getElementById('stat-avg-congestion');
        const avgPillEl = document.getElementById('avg-congestion');
        const formatted = avgCongestion.toFixed(2);
        if (avgEl) avgEl.textContent = formatted;
        if (avgPillEl) avgPillEl.textContent = formatted;

        // Hotspots
        const hotEl = document.getElementById('stat-hotspots');
        if (hotEl) hotEl.textContent = hotspotCount;

        // Grid update time
        const timeEl = document.getElementById('grid-update-time');
        if (timeEl) {
            timeEl.textContent = new Date().toLocaleTimeString('en-US', {
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            });
        }
    }

    // Handle traffic updates from WebSocket
    window.handleTrafficUpdate = function (data) {
        if (data.intersection_id) {
            // Single intersection update
            updateIntersectionCell(data);

            // Recalculate globals from DOM
            const cells = document.querySelectorAll('.intersection-cell');
            let tv = 0, scores = [], hs = 0;
            cells.forEach(cell => {
                const count = parseInt(cell.querySelector('.vehicle-count')?.textContent || '0');
                const score = parseFloat(cell.getAttribute('data-congestion') || '0');
                tv += count;
                scores.push(score);
                if (score >= 0.7) hs++;
            });
            totalVehicles = tv;
            avgCongestion = scores.length > 0 ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
            hotspotCount = hs;
            updateGlobalStats();
        }
    };
})();
