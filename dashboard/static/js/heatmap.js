/**
 * EdgeCloudX Dashboard — Canvas Heatmap Renderer
 * =================================================
 * Renders congestion heatmap on a canvas with smooth color interpolation.
 */

(function () {
    'use strict';

    const canvas = document.getElementById('heatmap-canvas');
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    const DPR = window.devicePixelRatio || 1;

    // High-DPI canvas setup
    function setupCanvas() {
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width * DPR;
        canvas.height = rect.height * DPR;
        ctx.scale(DPR, DPR);
    }
    setupCanvas();
    window.addEventListener('resize', setupCanvas);

    // Color interpolation for heatmap
    function scoreToColor(score) {
        // 0.0 = cool blue-green, 0.5 = yellow, 1.0 = hot red
        const s = Math.max(0, Math.min(1, score));

        let r, g, b;
        if (s < 0.25) {
            const t = s / 0.25;
            r = Math.round(20 + t * 30);
            g = Math.round(180 + t * 30);
            b = Math.round(140 - t * 40);
        } else if (s < 0.5) {
            const t = (s - 0.25) / 0.25;
            r = Math.round(50 + t * 200);
            g = Math.round(210 - t * 20);
            b = Math.round(100 - t * 60);
        } else if (s < 0.75) {
            const t = (s - 0.5) / 0.25;
            r = Math.round(250);
            g = Math.round(190 - t * 100);
            b = Math.round(40 - t * 20);
        } else {
            const t = (s - 0.75) / 0.25;
            r = Math.round(250 - t * 10);
            g = Math.round(90 - t * 60);
            b = Math.round(20 + t * 30);
        }

        return `rgb(${r}, ${g}, ${b})`;
    }

    function renderHeatmap(heatmapData) {
        if (!heatmapData || !heatmapData.length) return;

        const rect = canvas.getBoundingClientRect();
        const w = rect.width;
        const h = rect.height;
        const rows = heatmapData.length;
        const cols = heatmapData[0].length;
        const cellW = w / cols;
        const cellH = h / rows;
        const padding = 4;

        // Clear canvas
        ctx.clearRect(0, 0, w, h);

        // Draw background
        ctx.fillStyle = 'rgba(10, 14, 23, 0.9)';
        ctx.fillRect(0, 0, w, h);

        // Draw cells
        for (let row = 0; row < rows; row++) {
            for (let col = 0; col < cols; col++) {
                const score = heatmapData[row][col];
                const x = col * cellW + padding;
                const y = row * cellH + padding;
                const cw = cellW - padding * 2;
                const ch = cellH - padding * 2;

                // Cell background with rounded corners
                const radius = 8;
                ctx.beginPath();
                ctx.roundRect(x, y, cw, ch, radius);

                // Gradient fill
                const gradient = ctx.createRadialGradient(
                    x + cw / 2, y + ch / 2, 0,
                    x + cw / 2, y + ch / 2, Math.max(cw, ch) / 1.5
                );
                gradient.addColorStop(0, scoreToColor(score));
                gradient.addColorStop(1, scoreToColor(score * 0.7));
                ctx.fillStyle = gradient;
                ctx.fill();

                // Cell border
                ctx.strokeStyle = 'rgba(148, 163, 184, 0.15)';
                ctx.lineWidth = 1;
                ctx.stroke();

                // Score text
                ctx.fillStyle = score > 0.5 ? '#fff' : 'rgba(255, 255, 255, 0.85)';
                ctx.font = `bold ${Math.max(12, cellW * 0.14)}px Inter, sans-serif`;
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText(score.toFixed(2), x + cw / 2, y + ch / 2 - 6);

                // Label text
                ctx.fillStyle = 'rgba(255, 255, 255, 0.5)';
                ctx.font = `${Math.max(9, cellW * 0.09)}px Inter, sans-serif`;
                ctx.fillText(`${row},${col}`, x + cw / 2, y + ch / 2 + 12);
            }
        }

        // Update timestamp
        const timeEl = document.getElementById('heatmap-update-time');
        if (timeEl) {
            timeEl.textContent = new Date().toLocaleTimeString('en-US', {
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            });
        }
    }

    // Handle heatmap updates from WebSocket
    window.handleHeatmapUpdate = function (data) {
        const matrix = data.smoothed_heatmap || data.raw_heatmap || data.heatmap;
        if (matrix) {
            renderHeatmap(matrix);
        }
    };

    // Initial render with zeros
    const defaultRows = 4, defaultCols = 4;
    const initial = Array.from({ length: defaultRows }, () =>
        Array.from({ length: defaultCols }, () => 0.0)
    );
    renderHeatmap(initial);
})();
