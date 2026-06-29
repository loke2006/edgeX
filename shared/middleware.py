"""
EdgeCloudX Shared — RBAC Middleware
=====================================
Role-Based Access Control for FastAPI services.

Roles hierarchy:
  viewer   → read-only access
  operator → read + control signals + manage alerts
  admin    → full access including user management
  edge_node → publish-only (no API access)

Usage:
    from shared.middleware import require_role

    @app.post("/alerts/emergency")
    async def create_alert(
        alert: AlertSchema,
        user: dict = Depends(require_role("operator", "admin")),
    ):
        ...
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)


class Role(str, Enum):
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"
    EDGE_NODE = "edge_node"


# Permission hierarchy — higher roles include all lower permissions
ROLE_HIERARCHY = {
    Role.VIEWER: 0,
    Role.EDGE_NODE: 0,
    Role.OPERATOR: 1,
    Role.ADMIN: 2,
}


async def _verify_token_remote(token: str, auth_service_url: str) -> dict:
    """Verify JWT token by calling the auth-service /auth/verify endpoint."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{auth_service_url}/auth/verify",
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code == 200:
                return resp.json()
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
            )
    except httpx.RequestError as e:
        logger.error(f"Auth service unreachable: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable",
        )


def require_role(*allowed_roles: str):
    """
    FastAPI dependency that enforces role-based access control.

    Usage:
        @app.get("/admin-only")
        async def admin_endpoint(user=Depends(require_role("admin"))):
            ...
    """
    allowed = {r if isinstance(r, str) else r.value for r in allowed_roles}

    async def _dependency(
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    ) -> dict:
        if not credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Get auth service URL from app state or environment
        auth_url = getattr(request.app.state, "auth_service_url", None)
        if not auth_url:
            import os
            auth_url = os.environ.get("AUTH_SERVICE_URL", "http://auth-service:8000")

        user = await _verify_token_remote(credentials.credentials, auth_url)
        user_role = user.get("role", "viewer")

        if user_role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions. Required: {', '.join(allowed)}. Your role: {user_role}",
            )

        return user

    return _dependency


def add_security_headers(app):
    """
    Add security headers middleware to a FastAPI app.

    Call once during app setup:
        add_security_headers(app)
    """
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request as StarletteRequest
    from starlette.responses import Response

    class SecurityHeadersMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: StarletteRequest, call_next) -> Response:
            response = await call_next(request)
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
            return response

    app.add_middleware(SecurityHeadersMiddleware)
