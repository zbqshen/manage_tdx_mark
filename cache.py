#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDX 标记管理器缓存系统
===========================

此模块提供智能缓存层，通过减少冗余的文件I/O
和数据处理操作来提高性能。
"""

import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, TypeVar, Generic, Callable, List
from threading import RLock
from functools import wraps

from .constants import DEFAULT_CACHE_TTL, MAX_CACHE_SIZE
from .models import CacheEntry, CacheStatus

T = TypeVar('T')


class LRUCache(Generic[T]):
    """
    线程安全的LRU（最近最少使用）缓存实现
    """
    
    def __init__(self, max_size: int = MAX_CACHE_SIZE):
        """
        初始化LRU缓存
        
        Args:
            max_size: 要存储的最大条目数
        """
        self.max_size = max_size
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []
        self._lock = RLock()
    
    def get(self, key: str) -> Optional[T]:
        """
        从缓存中获取项目
        
        Args:
            key: 缓存键
            
        Returns:
            缓存的值，如果找不到或已过期则返回None
        """
        with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            # 检查是否过期
            if entry.is_expired:
                self._remove(key)
                return None
            
            # 更新访问顺序
            self._update_access_order(key)
            
            return entry.access()
    
    def put(self, key: str, value: T, ttl: int = DEFAULT_CACHE_TTL) -> None:
        """
        将项目放入缓存
        
        Args:
            key: 缓存键
            value: 要缓存的值
            ttl: 生存时间（秒）
        """
        with self._lock:
            # 如果已存在则移除
            if key in self._cache:
                self._remove(key)
            
            # Check if cache is full
            if len(self._cache) >= self.max_size:
                self._evict_lru()
            
            # Add new entry
            entry = CacheEntry(
                data=value,
                timestamp=datetime.now(),
                ttl=ttl
            )
            self._cache[key] = entry
            self._access_order.append(key)
    
    def invalidate(self, key: str) -> bool:
        """
        Remove item from cache
        
        Args:
            key: Cache key to remove
            
        Returns:
            True if item was removed, False if not found
        """
        with self._lock:
            if key in self._cache:
                self._remove(key)
                return True
            return False
    
    def clear(self) -> None:
        """Clear entire cache"""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()
    
    def size(self) -> int:
        """Get current cache size"""
        with self._lock:
            return len(self._cache)
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            total_accesses = sum(entry.access_count for entry in self._cache.values())
            expired_count = sum(1 for entry in self._cache.values() if entry.is_expired)
            
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'total_accesses': total_accesses,
                'expired_entries': expired_count,
                'hit_ratio': self._calculate_hit_ratio()
            }
    
    def _remove(self, key: str) -> None:
        """Remove key from cache and access order"""
        if key in self._cache:
            del self._cache[key]
        if key in self._access_order:
            self._access_order.remove(key)
    
    def _update_access_order(self, key: str) -> None:
        """Update access order for LRU tracking"""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
    
    def _evict_lru(self) -> None:
        """Evict least recently used item"""
        if self._access_order:
            lru_key = self._access_order[0]
            self._remove(lru_key)
    
    def _calculate_hit_ratio(self) -> float:
        """Calculate cache hit ratio"""
        # This is a simplified implementation
        # In a real scenario, you'd track hits/misses separately
        if not self._cache:
            return 0.0
        return min(1.0, len(self._cache) / self.max_size)


class CacheManager:
    """
    Central cache manager for different types of cached data
    """
    
    def __init__(self):
        """Initialize cache manager"""
        self._file_content_cache = LRUCache[str](max_size=10)
        self._parsed_data_cache = LRUCache[Dict](max_size=20)
        self._validation_cache = LRUCache[Dict](max_size=50)
        self._stats_cache = LRUCache[Dict](max_size=30)
        self._query_cache = LRUCache[Any](max_size=100)
        
        # Track cache hit/miss statistics
        self._hits = 0
        self._misses = 0
        self._lock = RLock()
    
    def get_file_content(self, file_path: str, file_hash: str) -> Optional[str]:
        """
        Get cached file content
        
        Args:
            file_path: Path to the file
            file_hash: Hash of the file for validation
            
        Returns:
            Cached content or None
        """
        cache_key = f"file_content:{file_path}:{file_hash}"
        content = self._file_content_cache.get(cache_key)
        
        with self._lock:
            if content is not None:
                self._hits += 1
            else:
                self._misses += 1
        
        return content
    
    def cache_file_content(self, file_path: str, file_hash: str, 
                          content: str, ttl: int = 300) -> None:
        """
        Cache file content
        
        Args:
            file_path: Path to the file
            file_hash: Hash of the file
            content: File content to cache
            ttl: Time to live in seconds
        """
        cache_key = f"file_content:{file_path}:{file_hash}"
        self._file_content_cache.put(cache_key, content, ttl)
    
    def get_parsed_data(self, content_hash: str) -> Optional[Dict]:
        """
        Get cached parsed data
        
        Args:
            content_hash: Hash of the content
            
        Returns:
            Cached parsed data or None
        """
        cache_key = f"parsed_data:{content_hash}"
        data = self._parsed_data_cache.get(cache_key)
        
        with self._lock:
            if data is not None:
                self._hits += 1
            else:
                self._misses += 1
        
        return data
    
    def cache_parsed_data(self, content_hash: str, data: Dict, 
                         ttl: int = 600) -> None:
        """
        Cache parsed data
        
        Args:
            content_hash: Hash of the content
            data: Parsed data to cache
            ttl: Time to live in seconds
        """
        cache_key = f"parsed_data:{content_hash}"
        self._parsed_data_cache.put(cache_key, data, ttl)
    
    def get_validation_result(self, data_hash: str) -> Optional[Dict]:
        """
        Get cached validation result
        
        Args:
            data_hash: Hash of the data
            
        Returns:
            Cached validation result or None
        """
        cache_key = f"validation:{data_hash}"
        result = self._validation_cache.get(cache_key)
        
        with self._lock:
            if result is not None:
                self._hits += 1
            else:
                self._misses += 1
        
        return result
    
    def cache_validation_result(self, data_hash: str, result: Dict,
                               ttl: int = 300) -> None:
        """
        Cache validation result
        
        Args:
            data_hash: Hash of the data
            result: Validation result to cache
            ttl: Time to live in seconds
        """
        cache_key = f"validation:{data_hash}"
        self._validation_cache.put(cache_key, result, ttl)
    
    def get_stats(self, data_hash: str) -> Optional[Dict]:
        """
        Get cached statistics
        
        Args:
            data_hash: Hash of the data
            
        Returns:
            Cached statistics or None
        """
        cache_key = f"stats:{data_hash}"
        stats = self._stats_cache.get(cache_key)
        
        with self._lock:
            if stats is not None:
                self._hits += 1
            else:
                self._misses += 1
        
        return stats
    
    def cache_stats(self, data_hash: str, stats: Dict, ttl: int = 300) -> None:
        """
        Cache statistics
        
        Args:
            data_hash: Hash of the data
            stats: Statistics to cache
            ttl: Time to live in seconds
        """
        cache_key = f"stats:{data_hash}"
        self._stats_cache.put(cache_key, stats, ttl)
    
    def get_query_result(self, query_hash: str) -> Optional[Any]:
        """
        Get cached query result
        
        Args:
            query_hash: Hash of the query
            
        Returns:
            Cached query result or None
        """
        cache_key = f"query:{query_hash}"
        result = self._query_cache.get(cache_key)
        
        with self._lock:
            if result is not None:
                self._hits += 1
            else:
                self._misses += 1
        
        return result
    
    def cache_query_result(self, query_hash: str, result: Any,
                          ttl: int = 180) -> None:
        """
        Cache query result
        
        Args:
            query_hash: Hash of the query
            result: Query result to cache
            ttl: Time to live in seconds
        """
        cache_key = f"query:{query_hash}"
        self._query_cache.put(cache_key, result, ttl)
    
    def invalidate_all(self) -> None:
        """Invalidate all caches"""
        self._file_content_cache.clear()
        self._parsed_data_cache.clear()
        self._validation_cache.clear()
        self._stats_cache.clear()
        self._query_cache.clear()
        
        with self._lock:
            self._hits = 0
            self._misses = 0
    
    def get_global_stats(self) -> Dict[str, Any]:
        """
        Get global cache statistics
        
        Returns:
            Dictionary with cache statistics
        """
        with self._lock:
            total_requests = self._hits + self._misses
            hit_ratio = self._hits / total_requests if total_requests > 0 else 0.0
            
            return {
                'total_requests': total_requests,
                'hits': self._hits,
                'misses': self._misses,
                'hit_ratio': hit_ratio,
                'caches': {
                    'file_content': self._file_content_cache.stats(),
                    'parsed_data': self._parsed_data_cache.stats(),
                    'validation': self._validation_cache.stats(),
                    'stats': self._stats_cache.stats(),
                    'query': self._query_cache.stats()
                }
            }


def cache_result(cache_manager: CacheManager, ttl: int = DEFAULT_CACHE_TTL):
    """
    Decorator to cache method results
    
    Args:
        cache_manager: Cache manager instance
        ttl: Time to live for cached result
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key = _generate_cache_key(func.__name__, args, kwargs)
            
            # Try to get from cache
            result = cache_manager.get_query_result(cache_key)
            if result is not None:
                return result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache_manager.cache_query_result(cache_key, result, ttl)
            
            return result
        return wrapper
    return decorator


def _generate_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """
    Generate a cache key from function name and arguments
    
    Args:
        func_name: Name of the function
        args: Positional arguments
        kwargs: Keyword arguments
        
    Returns:
        Cache key string
    """
    # Convert arguments to a stable string representation
    args_str = str(args)
    kwargs_str = str(sorted(kwargs.items()))
    combined = f"{func_name}:{args_str}:{kwargs_str}"
    
    # Hash to create a consistent key
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def calculate_file_hash(file_path: str) -> str:
    """
    Calculate hash of a file for cache validation
    
    Args:
        file_path: Path to the file
        
    Returns:
        SHA-256 hash of the file
    """
    try:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return ""


def calculate_data_hash(data: Any) -> str:
    """
    Calculate hash of data for cache validation
    
    Args:
        data: Data to hash
        
    Returns:
        SHA-256 hash of the data
    """
    try:
        import json
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(data_str.encode('utf-8')).hexdigest()
    except Exception:
        return ""


# Global cache manager instance
_global_cache_manager = None


def get_cache_manager() -> CacheManager:
    """
    Get the global cache manager instance
    
    Returns:
        Global cache manager
    """
    global _global_cache_manager
    if _global_cache_manager is None:
        _global_cache_manager = CacheManager()
    return _global_cache_manager


def reset_cache_manager() -> None:
    """Reset the global cache manager"""
    global _global_cache_manager
    if _global_cache_manager is not None:
        _global_cache_manager.invalidate_all()
    _global_cache_manager = None