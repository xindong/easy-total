// log4cxxconfigurator.h
#pragma once

#include <string>
#include <boost/scoped_ptr.hpp>

namespace log4cxx { 
	namespace helpers {
		class FileWatchdog;
	}
}

namespace Log4cxxConfigurator {

	typedef boost::scoped_ptr<log4cxx::helpers::FileWatchdog> FileWatchdogPtr;

	class PropertyWatchdog
	{
	public:
		PropertyWatchdog(const std::string & sPropertyFileName, long lDelayMs);
		~PropertyWatchdog();
	private:
		FileWatchdogPtr m_pImpl;
	};

	class XmlWatchdog
	{
	public:
		XmlWatchdog(const std::string & sXmlFileName, long lDelayMs);
		~XmlWatchdog();
	private:
		FileWatchdogPtr m_pImpl;
	};

}  // namespace Log4cxxConfigurator

