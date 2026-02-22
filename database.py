import aiosqlite
import asyncio
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class TaskStatus(Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Database:
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    dependencies TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    error_message TEXT
                )
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_status 
                ON tasks(status)
            """)
            
            await db.commit()
    
    async def create_task(self, task_id: str, task_type: str, duration_ms: int, 
                         dependencies: List[str]) -> bool:
        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute("""
                        INSERT INTO tasks (id, type, duration_ms, dependencies, status, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        task_id,
                        task_type,
                        duration_ms,
                        json.dumps(dependencies),
                        TaskStatus.QUEUED.value,
                        datetime.utcnow().isoformat()
                    ))
                    await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False
    
    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM tasks WHERE id = ?
            """, (task_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return self._row_to_dict(row)
                return None
    
    async def list_tasks(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM tasks ORDER BY created_at DESC
            """) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_dict(row) for row in rows]
    
    async def update_task_status(self, task_id: str, status: TaskStatus, 
                                 error_message: Optional[str] = None) -> bool:
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                updates = ["status = ?"]
                params = [status.value]
                
                if status == TaskStatus.RUNNING:
                    updates.append("started_at = ?")
                    params.append(datetime.utcnow().isoformat())
                elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                    updates.append("completed_at = ?")
                    params.append(datetime.utcnow().isoformat())
                
                if error_message:
                    updates.append("error_message = ?")
                    params.append(error_message)
                
                params.append(task_id)
                
                cursor = await db.execute(f"""
                    UPDATE tasks 
                    SET {', '.join(updates)}
                    WHERE id = ?
                """, params)
                
                await db.commit()
                return cursor.rowcount > 0
    
    async def claim_task_for_execution(self, task_id: str) -> bool:
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("""
                    UPDATE tasks 
                    SET status = ?, started_at = ?
                    WHERE id = ? AND status = ?
                """, (
                    TaskStatus.RUNNING.value,
                    datetime.utcnow().isoformat(),
                    task_id,
                    TaskStatus.QUEUED.value
                ))
                
                await db.commit()
                return cursor.rowcount > 0
    
    async def get_tasks_by_status(self, status: TaskStatus) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM tasks WHERE status = ?
            """, (status.value,)) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_dict(row) for row in rows]
    
    async def delete_all_tasks(self):
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("DELETE FROM tasks")
                await db.commit()

    async def reset_running_tasks(self):
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    UPDATE tasks 
                    SET status = ?, started_at = NULL
                    WHERE status = ?
                """, (TaskStatus.QUEUED.value, TaskStatus.RUNNING.value))
                await db.commit()
    
    def _row_to_dict(self, row: aiosqlite.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "type": row["type"],
            "duration_ms": row["duration_ms"],
            "dependencies": json.loads(row["dependencies"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "error_message": row["error_message"]
        }
