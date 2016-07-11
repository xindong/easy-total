#include "xTools.h"
#include "xXMLParser.h"

UINT stringTok(std::string s, std::string k, std::vector<std::string> &v)
{
	std::string::size_type len = s.length();
	std::string::size_type i = 0, j = 0;

	while (i<len)
	{
		i = s.find_first_not_of(k, i);
		if (i==std::string::npos) break;

		j = s.find_first_of(k, i);
		if (j==std::string::npos)
		{
			v.push_back(s.substr(i, s.length()-i));
			break;
		}
		else
		{
			v.push_back(s.substr(i, j-i));
			i = j+1;
		}
	}

	return v.size();
}

// 获取所有被分隔的字段,包括空
UINT stringTokAll(std::string s, std::string k, std::vector<std::string> &v)
{
	std::string::size_type len = s.length();
	std::string::size_type i = 0, j = 0;

	while (i<len)
	{
		j = s.find_first_of(k, i);
		if (j==std::string::npos)
		{
			v.push_back(s.substr(i, s.length()-i));
			break;
		}
		else
		{
			v.push_back(s.substr(i, j-i));
			i = j+1;
		}
	}

	return v.size();
}
