#pragma once

#include <string.h>
#include <map>
#include <vector>
#include <set>
#include <random>
#include <functional>
#include <sys/socket.h>
#include <stdarg.h>
#include <cassert>
#include "xDefine.h"
#include "xLog.h"

UINT stringTok(std::string s, std::string k, std::vector<std::string> &v);
UINT stringTokAll(std::string s, std::string k, std::vector<std::string> &v);

template<typename T>
inline void numTok(std::string s,std::string k,std::vector<T> &v)
{
	std::vector<std::string> sv;
	stringTok(s,k,sv);
	for(UINT n=0;n<sv.size();n++)
	{
		v.push_back(atoi(sv[n].c_str()));
	}
}

template<typename T>
inline void numTok(std::string s,std::string k,std::set<T> &v)
{
	std::vector<std::string> sv;
	stringTok(s,k,sv);
	for(UINT n=0;n<sv.size();n++)
	{
		v.insert(atoi(sv[n].c_str()));
	}
}

template <typename T>
inline T* constructInPlace(T* p)
{
	return new(p) T;
}

/*template <typename T>
inline void* constructInPlace(T* p)
{
	new (static_cast<void *>(p)) T();
}
*/

//#define SAFE_DELETE(p) do {delete p; p = NULL;} while(false)

//#define SAFE_DELETE_VEC(p) do {delete[] p; p = NULL;} while(false)

/*
inline void SAFE_CLOSE_HANDLE(HANDLE &h)
{
	XDBG("[Handle]close %p", h);
	CloseHandle(h);
	h = 0;
}
*/

inline int randBetween(int min, int max)
{
	if (max==min) return min;
	unsigned int gap = abs(max-min);
	int ret = max>min?min:max;
	ret += rand()%(gap+1);
	return ret;
}

inline bool selectByPercent(int value)
{
	if(value>=randBetween(1,100))
		return true;
	return false;
}
inline bool selectByThousand(int value)
{
	if(value>=randBetween(1,1000))
		return true;
	return false;
}
inline bool selectByTenThousand(int value)
{
	if(value>=randBetween(1,10000))
		return true;
	return false;
}

#define parseFormat(buffer, fmt) \
	va_list argptr;\
	va_start(argptr, fmt);\
	vsnprintf(buffer, sizeof(buffer), fmt, argptr);\
	va_end(argptr)

template<typename T, typename M>
inline void Clamp(T &val, const M &min, const M &max)
{
	if (val < min) { val = min; return; }
	if (val > max) { val = max; return; }
}

template<typename T>
inline T square(const T &val)
{
	return val * val;
}

template<typename T>
inline T cube(const T &val)
{
	return val * val * val;
}

struct Parser
{
	Parser()
	{
		map.clear();
	}
	void reset()
	{
		for (std::map<std::string, std::string>::iterator it=map.begin(); it!=map.end(); it++)
			it->second.clear();
	}
	void key(std::string s)
	{
		std::string str("");
		map.insert(std::make_pair(s, str));
	}
	std::string value(std::string s)
	{
		return map[s];
	}
	std::map<std::string, std::string> map;
};

inline void getLocalTime(tm &_tm)
{
	time_t _t = time(NULL);
	localtime_r(&_t, &_tm);
}

inline DWORD getNowTime()
{
    return (DWORD)time(NULL);
}

inline void getLocalTime(tm &_tm, const time_t &_t)
{
	localtime_r(&_t, &_tm);
}

inline void getLocalTime(tm &_tm, const UINT &_t)
{
	time_t t = _t;
	localtime_r(&t, &_tm);
}

inline void parseTime(const char *str, UINT &time)
{
	struct tm tm1;
	sscanf(str, "%4d-%2d-%2d %2d:%2d:%2d", &tm1.tm_year, &tm1.tm_mon, &tm1.tm_mday, &tm1.tm_hour, &tm1.tm_min, &tm1.tm_sec);
	tm1.tm_year -= 1900;
	tm1.tm_mon--;
	tm1.tm_isdst=-1;

	time = mktime(&tm1);
}

inline void timeNumToStr(UINT time,std::stringstream& str)
{
	str.str("");
	if(time==0)
	{
		str<<"0秒";
		return;
	}
	UINT day = time/60/60/24;
	if(day)
	{
		str<<day<<"天";
		time-=day*24*60*60;
	}
	UINT h=time/60/60;
	if(h)
	{
		str<<h<<"小时";
		time=time-h*60*60;
	}
	h=time/60;
	if(h)
	{
		str<<h<<"分钟";
		time=time-h*60;
	}
	h=time;
	if(h)
		str<<h<<"秒";
}

static const char *CHINESE_NUM_STR[] = 
{ "零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十", 
	"十一", "十二", "十三", "十四", "十五", "十六", "十七", "十八", "十九", "二十", 
	"二十一", "二十二", "二十三", "二十四", "二十五", "二十六", "二十七", "二十八", "二十九", "三十" };
inline const char *getChineseNumStr(UINT i)
{
	if (i < sizeof(CHINESE_NUM_STR) / sizeof(const char *))
		return CHINESE_NUM_STR[i];
	return "";
}

static const char* CHINESE_DAY[] = {"日","一","二","三","四","五","六"};
inline const char* getChineseDay(UINT i)
{
	if ( i<sizeof(CHINESE_NUM_STR)/sizeof(char*))
		return CHINESE_DAY[i];
	else
		return "";
}

inline QWORD genAccIDKey(QWORD zoneID, QWORD accid)
{
	return (zoneID << 48) | accid;
}

inline void addslashes(std::string &str)
{
	std::string::size_type p = str.find("\\\\\\");
	while (p != std::string::npos){
		str.replace(p, 3, "\\"); 
		p = str.find("\\\\\\");
	}      

	p = str.find('\'');
	while (p != std::string::npos){
		if (p == 0 || (p > 0 && str[p - 1] != '\\') || (p > 1 && str[p - 1] == '\\' && str[p - 2] == '\\'))
		{
			str.replace(p, 1, "\\\'"); 
			p = str.find('\'', p + 2);
		}
		else
			p = str.find('\'', p + 1);
	}      

	//去除最后一个'\' 
	if (!str.empty() && str[str.size() - 1] == '\\')
		str[str.size() - 1] = '0';
}

inline void addslashes(char *in, UINT len)
{
	std::string str(in, len);
	addslashes(str);
	strncpy(in, str.c_str(), len);
}

template <typename T>
inline void stringToNum(std::string str,const char* tok, T& target)
{
	std::vector<std::string> strvec;
	stringTok(str,tok,strvec);
	for(UINT n=0;n<strvec.size();n++)
	{
		target.push_back(atoi(strvec[n].c_str()));
	}
}

template <typename T>
T clamp(const T& x, const T& min, const T& max)
{
	return std::min<T>(std::max<T>(min , x), max);
};

//按频率分布随机选择一个返回下标
template<typename FreqT>
UINT randomSelect(const std::vector<FreqT>& distribution)
{
	UINT sumFreq = 0;
	typedef typename std::vector<FreqT>::const_iterator Iter;
	for (Iter iter = distribution.begin(); iter != distribution.end(); ++iter)
	{
		sumFreq += *iter;
	}

	const UINT r = randBetween(1, sumFreq);
	UINT ptSum = 1;
	for (UINT i = 0; i < distribution.size(); ++i)
	{
		if (r >= ptSum && r < ptSum + distribution[i])
		{
			return i;
		}
		ptSum += distribution[i];
	}

	return distribution.size();
}

template<typename HasFreq, typename FreqT>
UINT randomSelect(const std::vector<HasFreq>& distribution)
{
	std::vector<FreqT> distVec;
	typedef typename std::vector<HasFreq>::const_iterator Iter;
	for (Iter iter = distribution.begin(); iter != distribution.end(); ++iter)
	{
		distVec.push_back(iter->dist);
	}
	return randomSelect(distVec);
}

inline void replaceAll(std::string& str, const char* key, const char* newKey)
{
	std::size_t pos = 0;
	while ((pos = str.find(key, pos)) != std::string::npos)
	{
		str.replace(pos, 1, newKey);
	}
}

inline void require(bool condition)
{
#ifdef _DEBUG
  assert(condition);
#endif
}

inline bool checkDirValid(std::string dir)
{
    if (dir.empty()) return false;
    if (dir == ".") return false;
    if (dir == "..") return false;
    if (dir == ".svn") return false;
    return true;
}

