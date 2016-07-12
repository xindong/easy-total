#include <log4cxx/logger.h>
#include "fmt.h"

#define LOG_DEBUG_TO(name,msg) do {\
	LOG4CXX_DEBUG(::log4cxx::Logger::getLogger(name), msg); } while(0)
#define LOG_INFO_TO(name,msg) do {\
	LOG4CXX_INFO(::log4cxx::Logger::getLogger(name), msg); } while(0)
#define LOG_WARN_TO(name,msg) do {\
	LOG4CXX_WARN(::log4cxx::Logger::getLogger(name), msg); } while(0)
#define LOG_ERROR_TO(name,msg) do {\
	LOG4CXX_ERROR(::log4cxx::Logger::getLogger(name), msg); } while(0)
#define LOG_FATAL_TO(name,msg) do {\
	LOG4CXX_FATAL(::log4cxx::Logger::getLogger(name), msg); } while(0)



#define XDBG(...) do { LOG_DEBUG_TO("main", fmt_args(__VA_ARGS__)); } while(0)
#define XINF(...) do { LOG_INFO_TO("main", fmt_args(__VA_ARGS__)); } while(0)
#define XWRN(...) do { LOG_WARN_TO("main", fmt_args(__VA_ARGS__)); } while(0)
#define XERR(...) do { LOG_ERROR_TO("main", fmt_args(__VA_ARGS__)); } while(0)
#define XFTL(...) do { LOG_FATAL_TO("main", fmt_args(__VA_ARGS__)); } while(0)

#define XLOG XINF



