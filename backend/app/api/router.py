from fastapi import APIRouter

from app.api.routes import auth, entities, rules, admin, monitor

api_router = APIRouter()
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(entities.router, prefix="", tags=["entities"])
api_router.include_router(rules.router, prefix="", tags=["rules"])
api_router.include_router(monitor.router, prefix="", tags=["monitor"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])
