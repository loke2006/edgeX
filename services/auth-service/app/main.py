"""
EdgeCloudX Auth Service — Main Application
============================================
FastAPI microservice for JWT authentication and user management.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from sqlalchemy import Boolean, Column, DateTime, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
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


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    role = Column(String(50), default="operator")  # operator, admin, edge_node
    created_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# Security
# =============================================================================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=settings.jwt_expiration_minutes)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def verify_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
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
    role: str = Field("operator", description="Role: operator, admin, edge_node")


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    username: str
    role: str


class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


# =============================================================================
# App
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("EdgeCloudX Auth Service — Starting")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Auth database initialized")
    yield
    logger.info("Auth Service stopped")


app = FastAPI(
    title="EdgeCloudX Auth Service",
    description="JWT authentication and user management for EdgeCloudX platform",
    version="0.1.0",
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


# =============================================================================
# Routes
# =============================================================================
@app.post("/auth/register", response_model=UserResponse)
async def register(request: RegisterRequest):
    """Register a new user."""
    async with async_session() as session:
        # Check existing
        result = await session.execute(
            select(User).where((User.username == request.username) | (User.email == request.email))
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

        logger.info(f"User registered: {request.username} (role: {request.role})")
        return user


@app.post("/auth/login", response_model=TokenResponse)
async def login(request: LoginRequest):
    """Authenticate and get JWT access token."""
    async with async_session() as session:
        result = await session.execute(
            select(User).where(User.username == request.username)
        )
        user = result.scalar_one_or_none()

        if not user or not verify_password(request.password, user.hashed_password):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        if not user.is_active:
            raise HTTPException(status_code=403, detail="Account disabled")

        token = create_access_token(
            data={"sub": user.username, "role": user.role}
        )

        logger.info(f"User logged in: {user.username}")
        return TokenResponse(
            access_token=token,
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


@app.get("/health")
async def health():
    return {"service": settings.service_name, "status": "healthy", "version": "0.1.0"}


@app.get("/health/liveness")
async def liveness():
    return {"status": "alive"}


@app.get("/")
async def root():
    return {"service": "EdgeCloudX Auth Service", "version": "0.1.0", "docs": "/docs"}
