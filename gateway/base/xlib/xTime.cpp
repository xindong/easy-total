#include "xTime.h"
#include <time.h>
#include <sys/time.h>
#include "xTools.h"

xTime::xTime()
{
	now();
	_elapse = usec;
}

xTime::~xTime()
{
}

void xTime::now()
{
	usec = getCurUSec();
}

DWORD xTime::getCurSec()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);

	return tv.tv_sec;
}

QWORD xTime::getCurUSec()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);

	return ((QWORD)tv.tv_sec) * ONE_MILLION + tv.tv_usec;
}

QWORD xTime::getCurMSec()
{
  return getCurUSec()/1000;
}

time_t xTime::getDayStart(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return time - (tm.tm_hour * 60 + tm.tm_min) * 60 - tm.tm_sec;//直接计算比使用mktime函数效率更高
}

time_t xTime::getDayStart(DWORD year, DWORD month, DWORD day)
{
	if (year < 1970 || month < 1 || month > 12 || day > 31)
		return 0;

	struct tm tm;
	bzero(&tm, sizeof(tm));
	tm.tm_year = year - 1900;
	tm.tm_mon = month - 1;
	tm.tm_mday = day;
	return mktime(&tm);
}

//以周一早上0点为起始时间
time_t xTime::getWeekStart(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return time - ((((tm.tm_wday + 6) % 7) * 24 + tm.tm_hour) * 60 + tm.tm_min) * 60 - tm.tm_sec;
}

time_t xTime::getMonthStart(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return time - (((tm.tm_mday - 1) * 24 + tm.tm_hour) * 60 + tm.tm_min) * 60 - tm.tm_sec;
}

const DWORD MONTH_DAY_NUM[] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
bool xTime::isValidDate(DWORD year, DWORD month, DWORD day)
{
	if (month < 1 || month > 12 || day < 1 || day > 31)
		return false;

	return day <= MONTH_DAY_NUM[month] + ((month == 2 && isLeapYear(year)) ? 1 : 0);
}

bool xTime::isLeapYear(DWORD year)
{
	return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

bool xTime::isSameDay(time_t t1, time_t t2)
{
	return getDayStart(t1)==getDayStart(t2);
}
bool xTime::isSameWeek(time_t t1,time_t t2)
{
	return getWeekStart(t1)==getWeekStart(t2); 
}
bool xTime::isSameMonth(time_t t1,time_t t2)
{
	return getMonthStart(t1)==getMonthStart(t2);
}

int xTime::getDay(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return tm.tm_mday;
}

int xTime::getMonth(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return tm.tm_mon+1;
}

int xTime::getYear(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return tm.tm_year+1900;
}

int xTime::getWeek(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return tm.tm_wday;
}

int xTime::getHour(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return tm.tm_hour;
}

int xTime::getMin(time_t time)
{
	struct tm tm;
	getLocalTime(tm, time);
	return tm.tm_min;
}

void xTime::elapseStart()
{
	_elapse = getCurUSec();
}

DWORD xTime::elapse()
{
	return (DWORD)(uElapse() / ONE_MILLION);
}

bool xTime::elapse(DWORD s)
{
	if (elapse()>=s)
	{
		_elapse += s * ONE_MILLION;
		while (elapse()>=s)
			_elapse += s * ONE_MILLION;
		return true;
	}
	return false;
}

QWORD xTime::milliElapse()
{
	return uElapse()/1000;
}

bool xTime::milliElapse(QWORD m)
{
	if (milliElapse()>=m)
	{
		_elapse += m*1000;
		while (elapse()>=m)
			_elapse += m*1000;
		return true;
	}
	return false;
}

QWORD xTime::uElapse()
{
	QWORD cur = getCurUSec();

	if (cur>=_elapse)
		return cur-_elapse;
	else
		return 0;
}

bool xTime::uElapse(QWORD u)
{
	if (uElapse()>=u)
	{
		_elapse += u;
		while (elapse()>=u)
			_elapse += u;
		return true;
	}
	return false;
}

bool xTime::elapse(DWORD s,QWORD cur)
{
	s = s * ONE_MILLION;
	if( cur >= (_elapse+s) )
	{
		_elapse += s;
		while( cur >= (_elapse+s) )
			_elapse += s;
		return true;
	}
	return false;
}

bool xTime::milliElapse(QWORD m,QWORD cur)
{
	m = m*1000;
	if( cur>=(_elapse+m) )
	{
		_elapse += m;
		while( cur>=(_elapse+m) )
			_elapse += m;
		return true;
	}
	return false;
}

bool xTime::uElapse(QWORD u,QWORD cur)
{
	if( cur>=(_elapse+u) )
	{
		_elapse += u;
		while( cur>=(_elapse+u) )
			_elapse += u; 
		return true;
	}
	return false;
}

bool xTime::isBetween(WORD beginHour,WORD beginMin,WORD endHour,WORD endMin,DWORD curTime)
{
	DWORD dayElapse = curTime - getDayStart(curTime);
	return (dayElapse>=(DWORD)(beginHour*60*60+beginMin*60) && dayElapse<=(DWORD)(endHour*60*60+endMin*60));
}

bool xTime::getLastMonth(DWORD& year, DWORD& month)
{
    year  = (DWORD)(xTime::getYear(xTime::getCurSec()) );
    month = (DWORD)(xTime::getMonth(xTime::getCurSec()) );
    if (month == 1)
    {
        year--;
        month = 12;
    }
    else
    {
        month--;
    }

    return true;
}

std::string getAscTime(const DWORD &dwTime)
{
	char str[128];
	bzero(str, sizeof(str));

	struct tm tm;
	getLocalTime(tm, (time_t)time);

	snprintf(str, sizeof(str), "%u-%u-%u %u:%u", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min);

	return str;
}

bool xDayTimer::timeUp(struct tm &tm_cur)
{
	if (_last_done != tm_cur.tm_mday)
	{
		if (tm_cur.tm_hour == _hour)
		{
			if (tm_cur.tm_min <= _delay)
			{
				_last_done = tm_cur.tm_mday;
				return true;
			}
			_last_done = tm_cur.tm_mday;
		}
		return false;
	}
	return false;
}

