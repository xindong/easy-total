// log4cxxconfigurator.cpp
#include "log4cxxconfigurator.h"

#include <log4cxx/helpers/filewatchdog.h>
#include <log4cxx/logmanager.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::helpers;
using namespace log4cxx::xml;


void FixLevelBug()
{
	(void)log4cxx::Level::getAll();
	(void)log4cxx::Level::getDebug();
	(void)log4cxx::Level::getTrace();
	(void)log4cxx::Level::getInfo();
	(void)log4cxx::Level::getWarn();
	(void)log4cxx::Level::getError();
	(void)log4cxx::Level::getFatal();
	(void)log4cxx::Level::getOff();
}

namespace Log4cxxConfigurator
{

	class XmlWatchdogImp : public FileWatchdog
	{
	public:
		XmlWatchdogImp(const File & filename) : FileWatchdog(filename) {};

		virtual void doOnChange()
		{
			DOMConfigurator().doConfigure(file, LogManager::getLoggerRepository());
		}
	};

	class PropertyWatchdogImp : public FileWatchdog
	{
	public:
		explicit PropertyWatchdogImp(const File & filename) : FileWatchdog(filename) {};

		virtual void doOnChange()
		{
			PropertyConfigurator().doConfigure(file, LogManager::getLoggerRepository());
		}
	};

}  // namespace

namespace Log4cxxConfigurator 
{

	PropertyWatchdog::PropertyWatchdog(const std::string & sPropertyFileName, long lDelayMs)
		: m_pImpl(new PropertyWatchdogImp(File(sPropertyFileName)))  // scoped_ptr
	{
		m_pImpl->setDelay(lDelayMs);
		m_pImpl->start();
		FixLevelBug();
	}

	PropertyWatchdog::~PropertyWatchdog()
	{
		m_pImpl.reset();
		LogManager::shutdown();
	}

	XmlWatchdog::XmlWatchdog(const std::string & sXmlFileName, long lDelayMs)
		: m_pImpl(new XmlWatchdogImp(File(sXmlFileName)))  // scoped_ptr
	{
		m_pImpl->setDelay(lDelayMs);
		m_pImpl->start();
		FixLevelBug();
	}

	XmlWatchdog::~XmlWatchdog()
	{
		m_pImpl.reset();
		LogManager::shutdown();
	}

}  // namespace Log4cxxConfigurator

