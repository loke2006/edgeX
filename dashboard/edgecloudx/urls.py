"""
EdgeCloudX Dashboard — URL Configuration
"""

from django.urls import path

from traffic import views

urlpatterns = [
    path("", views.dashboard_view, name="dashboard"),
    path("api/health/", views.health_check, name="health"),
]
