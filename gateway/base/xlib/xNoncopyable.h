#ifndef _X_NONCOPYABLE
#define _X_NONCOPYABLE
struct xNoncopyable
{
public:
	xNoncopyable(){}
	virtual ~xNoncopyable(){}
	
private:
	xNoncopyable(const xNoncopyable &);
	xNoncopyable& operator=(const xNoncopyable &);
};


#endif

