import uuid
import logging
from fastapi import APIRouter, Depends, HTTPException, Form, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models import User
from app.auth import get_password_hash, verify_password, create_access_token
from app.deps import get_db, get_current_user, require_role

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["users"])


@router.post("/auth/register")
async def register(
    email: str = Form(...),
    password: str = Form(...),
    full_name: str = Form(...),
    role: str = Form(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    existing = await db.scalar(select(User).where(User.email == email))
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    user = User(
        email=email,
        password_hash=get_password_hash(password),
        full_name=full_name,
        role=role.upper(),
        is_active=True,
    )
    db.add(user)
    await db.commit()
    return {"message": "User created successfully", "email": email}


@router.post("/auth/login")
async def login(
    email: str = Form(...),
    password: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    user = await db.scalar(select(User).where(User.email == email))
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )
    if not user.is_active:
        raise HTTPException(status_code=400, detail="Account is inactive")
    token = create_access_token({
        "user_id": str(user.id),
        "email": user.email,
        "role": user.role,
        "full_name": user.full_name or "",
    })
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": str(user.id),
            "email": user.email,
            "full_name": user.full_name or "",
            "role": user.role,
        },
    }


@router.get("/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    return current_user


@router.get("/users")
async def list_users(
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    result = await db.scalars(select(User))
    users = result.all()
    return {
        "users": [
            {
                "id": str(u.id),
                "email": u.email,
                "full_name": u.full_name or "",
                "role": u.role,
                "is_active": u.is_active,
            }
            for u in users
        ]
    }


@router.put("/users/{user_id}/reset-password")
async def reset_user_password(
    user_id: str,
    new_password: str = Form(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    try:
        uid = uuid.UUID(user_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="User not found")
    user = await db.scalar(select(User).where(User.id == uid))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.password_hash = get_password_hash(new_password)
    await db.commit()
    return {"message": "Password reset successfully"}


@router.put("/users/{user_id}/toggle-active")
async def toggle_user_active(
    user_id: str,
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    if user_id == current_user["user_id"]:
        raise HTTPException(status_code=400, detail="Cannot disable your own account")
    try:
        uid = uuid.UUID(user_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="User not found")
    user = await db.scalar(select(User).where(User.id == uid))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.is_active = not user.is_active
    await db.commit()
    return {
        "message": f"User {'activated' if user.is_active else 'deactivated'} successfully",
        "is_active": user.is_active,
    }
