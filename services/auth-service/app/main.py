"""
EdgeCloudX Auth Service — Main Application
============================================
FastAPI microservice for JWT authentication, user management, and RBAC.

Enhanced with:
- Role-based access control (admin, operator, viewer, edge_node)
- User management endpoints (admin only)
- Audit logging for login, register, role changes
- JWT refresh endpoint
- Password strength validation
- Security headers
- Structured JSON logging
"""

import os
import re
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Optional

# Add shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from shared.logging import setup_logging  # noqa: E402

setup_logging("auth-service")

import logging  # noqa: E402

from fastapi import Depends, FastAPI, HTTPException, status  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer  # noqa: E402
from jose import JWTError, jwt  # noqa: E402
from passlib.context import CryptContext  # noqa: E402
from prometheus_fastapi_instrumentator import Instrumentator  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402
from pydantic_settings import BaseSettings  # noqa: E402
from shared.audit import AuditLogger  # noqa: E402
from shared.metrics import SERVICE_INFO  # noqa: E402
from shared.middleware import add_security_headers  # noqa: E402
from sqlalchemy import Boolean, Column, DateTime, Integer, String, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase  # noqa: E402

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================
class Settings(BaseSettings):
    service_name: str = "auth-service"
    debug: bool = False
    database_url: str = "postgresql+asyncpg://edgecloudx:edgecloudx_secret@postgres:5432/edgecloudx"
    jwt_secret_key: str = "dev-secret-key"
    jwt_algorithm: str = "HS256"
    jwt_expiration_minutes: int = 60
    jwt_refresh_expiration_minutes: int = 1440  # 24 hours

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

# =============================================================================
# Database
# =============================================================================
engine = create_async_engine(settings.database_url, echo=settings.debug, pool_size=5)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


VALID_ROLES = {"admin", "operator", "viewer", "edge_node"}


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    role = Column(String(50), default="viewer")  # admin, operator, viewer, edge_node
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# =============================================================================
# Security
# =============================================================================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def validate_password_strength(password: str) -> Optional[str]:
    """Validate password strength. Returns error message or None."""
    if len(password) < 8:
        return "Password must be at least 8 characters"
    if not re.search(r"[A-Z]", password):
        return "Password must contain at least one uppercase letter"
    if not re.search(r"[a-z]", password):
        return "Password must contain at least one lowercase letter"
    if not re.search(r"\d", password):
        return "Password must contain at least one digit"
    return None


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.jwt_expiration_minutes)
    )
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def create_refresh_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=settings.jwt_refresh_expiration_minutes
    )
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def verify_token(token: str, expected_type: str = "access") -> dict:
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        if payload.get("type") != expected_type:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid token type. Expected {expected_type}",
            )
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


# =============================================================================
# Schemas
# =============================================================================
class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=100)
    email: str = Field(..., max_length=255)
    password: str = Field(..., min_length=6)
    role: str = Field("viewer", description="Role: admin, operator, viewer, edge_node")


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    username: str
    role: str


class RefreshRequest(BaseModel):
    refresh_token: str


class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class RoleUpdateRequest(BaseModel):
    role: str = Field(..., description="New role: admin, operator, viewer, edge_node")


# =============================================================================
# App
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Auth Service starting")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Auth database initialized")

    # Initialize audit logger
    audit = AuditLogger(settings.database_url)
    await audit.init()
    app.state.audit = audit

    # Prometheus info
    SERVICE_INFO.info({"service": "auth-service", "version": "0.2.0"})

    yield
    logger.info("Auth Service stopped")


app = FastAPI(
    title="EdgeCloudX Auth Service",
    description="JWT authentication, user management, and RBAC for EdgeCloudX platform",
    version="0.2.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

add_security_headers(app)
Instrumentator().instrument(app).expose(app, include_in_schema=False)


# =============================================================================
# Dependencies
# =============================================================================
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to get current authenticated user from JWT token."""
    payload = verify_token(credentials.credentials)
    username = payload.get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    async with async_session() as session:
        result = await session.execute(select(User).where(User.username == username))
        user = result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user


async def require_admin(user: User = Depends(get_current_user)):
    """Dependency that requires admin role."""
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return user


# =============================================================================
# Routes
# =============================================================================
@app.post("/auth/register", response_model=UserResponse)
async def register(request: RegisterRequest):
    """Register a new user."""
    # Validate role
    if request.role not in VALID_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role. Must be one of: {', '.join(VALID_ROLES)}",
        )

    # Validate password strength
    pw_error = validate_password_strength(request.password)
    if pw_error:
        raise HTTPException(status_code=400, detail=pw_error)

    async with async_session() as session:
        # Check existing
        result = await session.execute(
            select(User).where(
                (User.username == request.username) | (User.email == request.email)
            )
        )
        if result.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Username or email already registered")

        user = User(
            username=request.username,
            email=request.email,
            hashed_password=hash_password(request.password),
            role=request.role,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)

        # Audit log
        if hasattr(app.state, "audit"):
            await app.state.audit.log(
                "user_registered",
                actor=request.username,
                resource=f"user:{user.id}",
                details={"role": request.role, "email": request.email},
                service="auth-service",
            )

        logger.info(
            "User registered",
            extra={"username": request.username, "role": request.role},
        )
        return user


@app.post("/auth/login", response_model=TokenResponse)
async def login(request: LoginRequest):
    """Authenticate and get JWT access + refresh tokens."""
    async with async_session() as session:
        result = await session.execute(
            select(User).where(User.username == request.username)
        )
        user = result.scalar_one_or_none()

        if not user or not verify_password(request.password, user.hashed_password):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        if not user.is_active:
            raise HTTPException(status_code=403, detail="Account disabled")

        token_data = {"sub": user.username, "role": user.role}
        access_token = create_access_token(token_data)
        refresh_token = create_refresh_token(token_data)

        # Audit log
        if hasattr(app.state, "audit"):
            await app.state.audit.log(
                "user_login",
                actor=user.username,
                resource=f"user:{user.id}",
                details={"role": user.role},
                service="auth-service",
            )

        logger.info("User logged in", extra={"username": user.username, "role": user.role})

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=settings.jwt_expiration_minutes * 60,
            username=user.username,
            role=user.role,
        )


@app.post("/auth/refresh", response_model=TokenResponse)
async def refresh_token(request: RefreshRequest):
    """Refresh access token using a refresh token."""
    payload = verify_token(request.refresh_token, expected_type="refresh")
    username = payload.get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    async with async_session() as session:
        result = await session.execute(select(User).where(User.username == username))
        user = result.scalar_one_or_none()
        if not user or not user.is_active:
            raise HTTPException(status_code=401, detail="User not found or inactive")

        token_data = {"sub": user.username, "role": user.role}
        new_access = create_access_token(token_data)
        new_refresh = create_refresh_token(token_data)

        return TokenResponse(
            access_token=new_access,
            refresh_token=new_refresh,
            expires_in=settings.jwt_expiration_minutes * 60,
            username=user.username,
            role=user.role,
        )


@app.get("/auth/verify", response_model=UserResponse)
async def verify(user: User = Depends(get_current_user)):
    """Verify JWT token and return current user info."""
    return user


@app.get("/auth/me", response_model=UserResponse)
async def me(user: User = Depends(get_current_user)):
    """Get current user profile."""
    return user


@app.get("/auth/users", response_model=list[UserResponse])
async def list_users(admin: User = Depends(require_admin)):
    """List all users (admin only)."""
    async with async_session() as session:
        result = await session.execute(select(User).order_by(User.created_at))
        users = result.scalars().all()
        return users


@app.put("/auth/users/{user_id}/role")
async def update_user_role(
    user_id: int,
    request: RoleUpdateRequest,
    admin: User = Depends(require_admin),
):
    """Update a user's role (admin only)."""
    if request.role not in VALID_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role. Must be one of: {', '.join(VALID_ROLES)}",
        )

    async with async_session() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        old_role = user.role
        user.role = request.role
        await session.commit()

        # Audit log
        if hasattr(app.state, "audit"):
            await app.state.audit.log(
                "role_changed",
                actor=admin.username,
                resource=f"user:{user.id}:{user.username}",
                details={"old_role": old_role, "new_role": request.role},
                service="auth-service",
            )

        logger.info(
            "User role updated",
            extra={
                "target_user": user.username,
                "old_role": old_role,
                "new_role": request.role,
                "by": admin.username,
            },
        )

        return {"user_id": user_id, "username": user.username, "old_role": old_role, "new_role": request.role}


@app.put("/auth/users/{user_id}/deactivate")
async def deactivate_user(user_id: int, admin: User = Depends(require_admin)):
    """Deactivate a user account (admin only)."""
    async with async_session() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        user.is_active = False
        await session.commit()

        if hasattr(app.state, "audit"):
            await app.state.audit.log(
                "user_deactivated",
                actor=admin.username,
                resource=f"user:{user.id}:{user.username}",
                service="auth-service",
            )

        return {"user_id": user_id, "status": "deactivated"}


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.2.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Auth Service", "version": "0.2.0", "docs": "/docs"}
