#ifndef __BASE_FMT_H__
#define __BASE_FMT_H__

#include <stdarg.h>
#include <string.h>
//#include <boost/format.hpp>

namespace {

/*boost::format Fmt(const std::string & sFmt)
{
	boost::format fmter(sFmt);
#ifdef NDEBUG
	fmter.exceptions(boost::io::no_error_bits);
#endif
	return fmter;
}*/

std::string fmt_args(const char * fmt,...)
{
        char msg[2048] = {0};
        va_list arg;
        va_start(arg, fmt);
        vsnprintf(msg, sizeof(msg), fmt, arg);
        va_end(arg);
        return std::string(msg);    
}

}	//namespace

#endif

