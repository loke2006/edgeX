"""
EdgeCloudX Dashboard — Views
===============================
Dashboard views that fetch initial data from backend microservices
and render the real-time dashboard template.
"""

import logging

import httpx
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render

logger = logging.getLogger(__name__)


async def dashboard_view(request):
    """
    Main dashboard view — fetches initial state from microservices
    and renders the dashboard template.
    """
    context = {
        "grid_rows": 4,
        "grid_cols": 4,
        "intersections": [],
        "services_health": [],
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        # Fetch traffic grid state
        try:
            resp = await client.get(
                f"{settings.TRAFFIC_SERVICE_URL}/traffic/grid"
            )
            if resp.status_code == 200:
                grid_data = resp.json()
                context["grid_rows"] = grid_data.get("grid_rows", 4)
                context["grid_cols"] = grid_data.get("grid_cols", 4)
                context["intersections"] = grid_data.get(
                    "intersections", []
                )
        except Exception as e:
            logger.warning(f"Failed to fetch traffic grid: {e}")

        # Fetch service health statuses
        services = [
            ("Traffic", settings.TRAFFIC_SERVICE_URL),
            ("Routing", settings.ROUTING_SERVICE_URL),
            ("Analytics", settings.ANALYTICS_SERVICE_URL),
            ("Alerts", settings.ALERT_SERVICE_URL),
            ("Auth", settings.AUTH_SERVICE_URL),
        ]

        for name, url in services:
            try:
                resp = await client.get(f"{url}/health")
                status = "healthy" if resp.status_code == 200 else "unhealthy"
            except Exception:
                status = "unreachable"
            context["services_health"].append({
                "name": name,
                "status": status,
            })

    return render(request, "traffic/dashboard.html", context)


async def health_check(request):
    """Dashboard health check endpoint."""
    return JsonResponse({
        "service": "dashboard",
        "status": "healthy",
        "version": "0.1.0",
    })
