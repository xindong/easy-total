#pragma once
#include <time.h>
#include <string>
#include "xDefine.h"

class xTime
{
	public:
		xTime();
		~xTime();

		static QWORD getCurUSec();
		static DWORD getCurSec();
        static QWORD getCurMSec();
		static time_t	getDayStart(time_t time);
		static time_t	getDayStart(DWORD year, DWORD month, DWORD day);
		static time_t	getWeekStart(time_t time);
		static time_t	getMonthStart(time_t time);
		static bool		isSameDay(time_t t1, time_t t2);
		static bool		isSameWeek(time_t t1,time_t t2);
		static bool		isSameMonth(time_t t1,time_t t2);
		static bool		isValidDate(DWORD year, DWORD month, DWORD day);	//检查日期是否合法
		static bool		isLeapYear(DWORD year);
		static int		getDay(time_t time);
		static int		getMonth(time_t time);
		static int		getYear(time_t time);
		static int		getWeek(time_t time);
		static int		getHour(time_t time);
		static int		getMin(time_t time);
		static bool		isBetween(WORD beginHour,WORD beginMin,WORD endHour,WORD endMin,DWORD curTime);
        static bool     getLastMonth(DWORD& year, DWORD& month);

		void now();

		void elapseStart();

		DWORD elapse();
		QWORD milliElapse();
		QWORD uElapse();

		bool elapse(DWORD s);
		bool milliElapse(QWORD m);
		bool uElapse(QWORD u);

		//cur均以微秒为单位
		bool elapse(DWORD s,QWORD cur);
		bool milliElapse(QWORD m,QWORD cur);
		bool uElapse(QWORD u,QWORD cur);

		QWORD usec;
		QWORD _elapse;
};

class xTimer
{
	public:
		xTimer(DWORD t) { elapse = t; }
		~xTimer(){}
		bool timeUp(QWORD cur) { return time.elapse(elapse, cur * ONE_MILLION); }
		void reset() { time.elapseStart(); }
		inline DWORD getElapse() const { return elapse; }
	private:
		xTime time;
		DWORD elapse;
};

class xMilliTimer
{
	public:
		xMilliTimer(DWORD t) { elapse = t; }
		~xMilliTimer(){}
		bool timeUp(QWORD cur) { return time.milliElapse(elapse,cur); }
		void reset() { time.elapseStart(); }
	private:
		xTime time;
		DWORD elapse;
};

//定时器  以天为周期  可设置每天几点执行一次
class xDayTimer
{
	public:
		xDayTimer(BYTE h, BYTE d=5)
		{
			_hour = h;
			_last_done = 0;
			_delay = d;
		}
		~xDayTimer() {}
		bool timeUp(struct tm &tm_cur);

	private:
		BYTE _hour;
		BYTE _last_done;
		BYTE _delay;
};

std::string getAscTime(const DWORD &dwTime);

