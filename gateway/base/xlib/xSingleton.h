#pragma once
#include "xTools.h"

template <typename T>
class xSingleton
{
	private:
		xSingleton(const xSingleton &);
		const xSingleton &operator=(const xSingleton &);

	protected:
		static T *_instance;
		xSingleton(){
			static CGarbo garbo;
		}
		virtual ~xSingleton(){}

	public:
/*		static void delMe()
		{
			SAFE_DELETE(_instance);
		}
		*/
		static T* instance()
		{
			if (!_instance)
				_instance = new T;
			return _instance;
		}
		static T& getMe()
		{
			if (!_instance)
				_instance = new T;
			return *_instance;
		}
		class CGarbo
		{
			public:
				~CGarbo()
				{
					SAFE_DELETE(xSingleton::_instance);
				}
				CGarbo()
				{
				}
		};
};

template <typename T>
T* xSingleton<T>::_instance = 0;
