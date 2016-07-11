#ifndef _X_MUTEX
#define _X_MUTEX

#include <string.h>
#include <pthread.h>
#include "xNoncopyable.h"
#include "xLog.h"

class xMutex : xNoncopyable
{
	pthread_mutex_t mutex;

public:
	xMutex(int type = PTHREAD_MUTEX_FAST_NP)
	{
		pthread_mutexattr_t attr;
		::pthread_mutexattr_init(&attr);
		::pthread_mutexattr_settype(&attr, type);
		::pthread_mutex_init(&mutex, &attr);
	}
	~xMutex()
	{
		::pthread_mutex_destroy(&mutex);
	}

	void lock()
	{
		::pthread_mutex_lock(&mutex);
	}

	void unlock()
	{
		::pthread_mutex_unlock(&mutex);
	}
};

class ScopeMutex
{
	xMutex &mutex;

public:
	ScopeMutex(xMutex &m) : mutex(m)
	{
		mutex.lock();
	}

	~ScopeMutex()
	{
		mutex.unlock();
	}
};

class xRWLock : xNoncopyable
{
	pthread_rwlock_t rwlock;

public:
	xRWLock()
	{
		int ret = ::pthread_rwlock_init(&rwlock, NULL);
		if (0 != ret)
			XERR("[读写锁]初始化失败 %s", strerror(errno));
	}
	~xRWLock()
	{
		::pthread_rwlock_destroy(&rwlock);
	}
	
	void rdlock()
	{
		::pthread_rwlock_rdlock(&rwlock);
	}
	
	void wrlock()
	{
		::pthread_rwlock_wrlock(&rwlock);
	}
	
	void unlock()
	{
		::pthread_rwlock_unlock(&rwlock);
	}
};

class ScopeReadLock
{
	xRWLock &lock;

public:
	ScopeReadLock(xRWLock &m) : lock(m)
	{
		lock.rdlock();
	}
	~ScopeReadLock()
	{
		lock.unlock();
	}
};

class ScopeWriteLock
{
	xRWLock &lock;

public:
	ScopeWriteLock(xRWLock &m) : lock(m)
	{
		lock.wrlock();
	}
	~ScopeWriteLock()
	{
		lock.unlock();
	}
};

#endif

