import asyncio
from typing import Set, Dict, List
from datetime import datetime
import logging

from database import Database, TaskStatus
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskScheduler:
    
    def __init__(self, database: Database):
        self.db = database
        self.max_concurrent = config.MAX_CONCURRENT_TASKS
        self.poll_interval = config.SCHEDULER_POLL_INTERVAL
        
        self._execution_semaphore = asyncio.Semaphore(self.max_concurrent)
        self._running_tasks: Set[str] = set()
        self._running_lock = asyncio.Lock()
        self._running = False
        self._scheduler_task: asyncio.Task = None
    
    async def start(self):
        if self._running:
            logger.warning("Scheduler is already running")
            return
        
        logger.info("Starting task scheduler...")
        
        await self._crash_recovery()
        
        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info(f"Scheduler started with max_concurrent={self.max_concurrent}")
    
    async def stop(self):
        if not self._running:
            return
        
        logger.info("Stopping scheduler...")
        self._running = False
        
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Scheduler stopped")
    
    async def _crash_recovery(self):
        # anything that was RUNNING when we died gets re-queued
        logger.info("Running crash recovery...")
        await self.db.reset_running_tasks()
    
    async def _scheduler_loop(self):
        try:
            while self._running:
                try:
                    await self._schedule_eligible_tasks()
                    await asyncio.sleep(self.poll_interval)
                except Exception as e:
                    logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                    await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info("Scheduler loop cancelled")
            raise
    
    async def _schedule_eligible_tasks(self):
        queued_tasks = await self.db.get_tasks_by_status(TaskStatus.QUEUED)

        if not queued_tasks:
            return

        completed_tasks = await self.db.get_tasks_by_status(TaskStatus.COMPLETED)
        completed_ids = {task['id'] for task in completed_tasks}

        failed_tasks = await self.db.get_tasks_by_status(TaskStatus.FAILED)
        failed_ids = {task['id'] for task in failed_tasks}

        for task in queued_tasks:
            task_id = task['id']
            dependencies = task['dependencies']

            if failed_ids and any(dep in failed_ids for dep in dependencies):
                logger.warning(f"Task {task_id} blocked by failed dep, marking failed")
                await self.db.update_task_status(
                    task_id, TaskStatus.FAILED,
                    error_message="dependency failed"
                )
                continue

            async with self._running_lock:
                if task_id in self._running_tasks:
                    continue
                if len(self._running_tasks) >= self.max_concurrent:
                    break

            if self._are_dependencies_satisfied(dependencies, completed_ids):
                await self._execute_task(task)
    
    def _are_dependencies_satisfied(self, dependencies: List[str], 
                                   completed_ids: Set[str]) -> bool:
        if not dependencies:
            return True
        
        return all(dep_id in completed_ids for dep_id in dependencies)
    
    async def _execute_task(self, task: Dict):
        task_id = task['id']
        
        claimed = await self.db.claim_task_for_execution(task_id)
        if not claimed:
            return
        
        async with self._running_lock:
            self._running_tasks.add(task_id)
        
        asyncio.create_task(self._run_task_with_semaphore(task))
    
    async def _run_task_with_semaphore(self, task: Dict):
        task_id = task['id']
        
        try:
            async with self._execution_semaphore:
                await self._run_task(task)
        finally:
            async with self._running_lock:
                self._running_tasks.discard(task_id)
    
    async def _run_task(self, task: Dict):
        task_id = task['id']
        duration_ms = task['duration_ms']
        
        logger.info(f"Task {task_id} started (duration={duration_ms}ms)")
        
        try:
            await asyncio.sleep(duration_ms / 1000.0)
            
            await self.db.update_task_status(task_id, TaskStatus.COMPLETED)
            logger.info(f"Task {task_id} completed successfully")
            
        except Exception as e:
            error_msg = f"Task execution failed: {str(e)}"
            await self.db.update_task_status(
                task_id, 
                TaskStatus.FAILED,
                error_message=error_msg
            )
            logger.error(f"Task {task_id} failed: {e}", exc_info=True)
    
    async def get_status(self) -> Dict:
        async with self._running_lock:
            running_count = len(self._running_tasks)
        
        return {
            "running": self._running,
            "max_concurrent": self.max_concurrent,
            "current_running": running_count,
            "available_slots": self.max_concurrent - running_count
        }
    
    async def validate_task_dependencies(self, task_id: str,
                                        dependencies: List[str]) -> tuple[bool, str]:
        if task_id in dependencies:
            return False, "Task cannot depend on itself"
        
        for dep_id in dependencies:
            dep_task = await self.db.get_task(dep_id)
            if not dep_task:
                return False, f"Dependency task '{dep_id}' does not exist"
        
        if await self._would_create_cycle(task_id, dependencies):
            return False, "Dependencies would create a circular dependency"
        
        return True, ""
    
    async def _would_create_cycle(self, new_task_id: str, 
                                  new_dependencies: List[str]) -> bool:
        all_tasks = await self.db.list_tasks()
        graph: Dict[str, List[str]] = {}
        
        for task in all_tasks:
            graph[task['id']] = task['dependencies']
        
        graph[new_task_id] = new_dependencies
        
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            neighbors = graph.get(node, [])
            for neighbor in neighbors:
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        return has_cycle(new_task_id)
