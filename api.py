from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

from database import Database
from scheduler import TaskScheduler
from models import (
    TaskSubmitRequest, 
    TaskResponse, 
    TaskListResponse,
    SubmitResponse
)
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db: Database = None
scheduler: TaskScheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, scheduler
    
    logger.info("Starting application...")
    
    db = Database(config.DATABASE_PATH)
    await db.initialize()
    logger.info(f"Database initialized at {config.DATABASE_PATH}")
    
    scheduler = TaskScheduler(db)
    await scheduler.start()
    
    logger.info("Application started successfully")
    
    yield
    
    logger.info("Shutting down application...")
    if scheduler:
        await scheduler.stop()
    logger.info("Application shutdown complete")


app = FastAPI(
    title="Task Scheduler",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "database": config.DATABASE_PATH,
        "max_concurrent_tasks": config.MAX_CONCURRENT_TASKS
    }


@app.get("/scheduler/status")
async def get_scheduler_status():
    status_info = await scheduler.get_status()
    return status_info


@app.post("/tasks", response_model=SubmitResponse, status_code=status.HTTP_201_CREATED)
async def submit_task(task_request: TaskSubmitRequest):
    is_valid, error_msg = await scheduler.validate_task_dependencies(
        task_request.id,
        task_request.dependencies
    )
    
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_msg
        )
    
    success = await db.create_task(
        task_id=task_request.id,
        task_type=task_request.type,
        duration_ms=task_request.duration_ms,
        dependencies=task_request.dependencies
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Task with ID '{task_request.id}' already exists"
        )
    
    logger.info(f"Task {task_request.id} submitted successfully")
    
    return SubmitResponse(
        message="Task submitted successfully",
        task_id=task_request.id
    )


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task_status(task_id: str):
    task = await db.get_task(task_id)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found"
        )
    
    return TaskResponse(**task)


@app.get("/tasks", response_model=TaskListResponse)
async def list_tasks(status_filter: str = None):
    if status_filter:
        valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]
        if status_filter.upper() not in valid_statuses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
            )
        
        from database import TaskStatus
        tasks = await db.get_tasks_by_status(TaskStatus[status_filter.upper()])
    else:
        tasks = await db.list_tasks()
    
    return TaskListResponse(
        tasks=[TaskResponse(**task) for task in tasks],
        total=len(tasks)
    )


@app.delete("/tasks")
async def clear_all_tasks():
    await db.delete_all_tasks()
    return {"status": "ok"}


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": str(exc)
        }
    )
