"""
EdgeCloudX Dashboard — WebSocket URL Routing
"""

from django.urls import re_path

from traffic import consumers

websocket_urlpatterns = [
    re_path(r"ws/traffic/$", consumers.TrafficConsumer.as_asgi()),
    re_path(r"ws/heatmap/$", consumers.HeatmapConsumer.as_asgi()),
    re_path(r"ws/alerts/$", consumers.AlertConsumer.as_asgi()),
    re_path(r"ws/ev/$", consumers.EVTrackerConsumer.as_asgi()),
    re_path(r"ws/nodes/$", consumers.NodeHealthConsumer.as_asgi()),
    re_path(r"ws/signals/$", consumers.SignalConsumer.as_asgi()),
]

