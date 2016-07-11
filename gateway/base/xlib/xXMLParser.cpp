#include <iconv.h>
#include "xXMLParser.h"
#include "xTools.h"

iconv_t xXMLParser::iconv_utf8_gbk;
iconv_t xXMLParser::iconv_gbk_utf8;

xXMLParser::xXMLParser()
{
	doc = 0;
	docBuffer = NULL;
}

xXMLParser::~xXMLParser()
{
	if (doc) xmlFreeDoc(doc);
	if (docBuffer) free(docBuffer);
#ifdef _LX_DEBUG
	XLOG("[xXMLParser],析构");
#endif
}

void xXMLParser::initSystem()
{
	xmlInitParser();
	XLOG("[xXMLParser],初始化");
	/*
	LIBXML_TEST_VERSION;
	iconv_utf8_gbk = iconv_open("UTF-8", "GBK");
	iconv_gbk_utf8 = iconv_open("GBK", "UTF-8");

	xmlCharEncodingHandlerPtr h = 0;
	h = xmlNewCharEncodingHandler("GB2312", gbk_input, gbk_output);//添加gb2312编码支持
	if (!h)
		XERR("注册GB2312编码失败");
	h = xmlNewCharEncodingHandler("GBK", gbk_input, gbk_output);//添加gbk编码支持
	if (!h)
		XERR("注册GBK编码失败");
	XLOG("[XML]注册解码器 GB2312 GBK");
	*/
}

void xXMLParser::clearSystem()
{
	/*
	iconv_close(iconv_gbk_utf8);
	iconv_close(iconv_utf8_gbk);
	*/
	xmlCleanupParser();
	xmlMemoryDump();
}

int xXMLParser::gbk_input(unsigned char *out, int *outlen, const unsigned char *in, int *inlen)
{
	char           *outbuf = (char *) out;
	char           *inbuf = (char *) in;
	size_t          rslt;
	XDBG("[XML]gbk_input 转码前 %d %s", *inlen, in);
	rslt =
		iconv (iconv_utf8_gbk, &inbuf, (size_t *) inlen,
				&outbuf, (size_t *) outlen);
	if (rslt < 0)
	{
		XERR("[XML]gbk_input 转码失败 %d", rslt);
		return rslt;
	}
	XDBG("[XML]gbk_input 转码后 %d %s", *outlen, out);
	*outlen = ((unsigned char *) outbuf - out);
	*inlen = ((unsigned char *) inbuf - in);
	XDBG("[XML]gbk_input 转码 ret=%d inlen=%d outlen=%d", rslt, *inlen, *outlen);
	return *outlen;
}

int xXMLParser::gbk_output(unsigned char *out, int *outlen, const unsigned char *in, int *inlen)
{
	char           *outbuf = (char *) out;
	char           *inbuf = (char *) in;
	size_t          rslt;
	rslt =
		iconv (iconv_gbk_utf8, &inbuf, (size_t *) inlen,
				&outbuf, (size_t *) outlen);
	if (rslt < 0)
	{
		XERR("[XML]gbk_output 转码失败 %d", rslt);
		return rslt;
	}
	*outlen = ((unsigned char *) outbuf - out);
	*inlen = ((unsigned char *) inbuf - in);
	XDBG("[XML]gbk_output 转码 ret=%d inlen=%d outlen=%d", rslt, *inlen, *outlen);
	return *outlen;
}


bool xXMLParser::parseDoc(const char *file)
{
	xmlKeepBlanksDefault(1);
	FILE *in = fopen(file, "r");
	if (in == NULL) return false;

	//doc = xmlReadFile(file, "GBK", 0);

	fseek(in, 0L, SEEK_END);
	DWORD numBytes = ftell(in);
	fseek(in, 0L, SEEK_SET);

	do 
	{
		docBuffer = (char*)calloc(numBytes, sizeof(char));
		if (docBuffer == NULL)
			break;

		size_t readsize = fread(docBuffer, sizeof(char), numBytes, in);
		if (readsize != numBytes)
			break;

		fclose(in);
		doc = xmlParseMemory(docBuffer, numBytes);

		if (!doc)
		{
			XERR("can not parse xml file %s", file);
			return false;
		}
		return true;
	} while (false);

	fclose(in);
	return false;
}

int xXMLParser::code_convert(const char *from_charset, const char *to_charset,char *inbuf,size_t inlen,char *outbuf,size_t outlen)
{
	iconv_t cd;
	char **pin = &inbuf;
	char **pout = &outbuf;

	cd = iconv_open(to_charset,from_charset);
	if (cd==0) return -1;
	memset(outbuf,0,outlen);
	if (iconv(cd, pin, (size_t *)&inlen, pout, (size_t *)&outlen) == (DWORD) -1)
		return -1;
	iconv_close(cd);
	return 0;
}

/*
bool xXMLParser::toUTF8()
{
	if (!doc) return false;
	xmlNodePtr root = getRoot();
	if (!root) return false;

	xmlChar *utf = xmlNodeGetContent(root);//需要释放内存
	XDBG("xml 转码前 %s", (char *)utf);
	size_t len = xmlStrlen(utf);
	char *out = (char *) xmlMalloc(len);
	bzero(out, len);
	if (0==code_convert("GB2312", "UTF-8", (char *)utf, xmlStrlen(utf), out, len))
	{
		XDBG("xml 转码后 %s", out);
		bcopy(out, (void *)utf, len);
		return true;
	}
	else
		return false;
}
*/

xmlNodePtr xXMLParser::getRoot()
{
	if (!doc) return 0;
	return xmlDocGetRootElement(doc);
}

xmlNodePtr xXMLParser::getChild(xmlNodePtr node)
{
	if (!node) return 0;
	return node->xmlChildrenNode;
}

xmlNodePtr xXMLParser::getChild(xmlNodePtr node, const char *name)
{
	if (!node) return 0;

	xmlNodePtr tmp = node->xmlChildrenNode;
	while (tmp)
	{
		if (tmp->type==XML_ELEMENT_NODE)
		{
			if (!xmlStrcmp(tmp->name, (const xmlChar *)name))
				return tmp;
		}
		tmp = getNext(tmp);
	}
	return 0;
}

DWORD xXMLParser::getChildNum(xmlNodePtr node,const char *name)
{
	if (!node) return 0;

	DWORD num = 0;
	xmlNodePtr tmp = node->xmlChildrenNode;
	while (tmp)
	{
		if (tmp->type == XML_ELEMENT_NODE)
		{
			if (!xmlStrcmp(tmp->name, (const xmlChar *)name))
				++num;
		}
		tmp = getNext(tmp);
	}
	return num;
}

xmlNodePtr xXMLParser::getNext(xmlNodePtr node)
{
	if (!node) return 0;

	xmlNodePtr tmp = node->next;
	while (tmp)
	{
		if (tmp->type==XML_ELEMENT_NODE)
			return tmp;
		tmp = tmp->next;
	}
	return 0;
}

xmlNodePtr xXMLParser::getNext(xmlNodePtr node, const char *name)
{
	if (!node) return 0;

	xmlNodePtr tmp = node->next;
	while (tmp)
	{
		if (tmp->type==XML_ELEMENT_NODE)
		{
			if (!xmlStrcmp(tmp->name, (const xmlChar *)name))
				return tmp;
		}
		tmp = getNext(tmp);
	}
	return 0;
}

xmlNodePtr xXMLParser::getNodeByPath(const char *path)
{
	std::vector<std::string> v;
	stringTok(path, "/", v);
	if(v.size()<=1) return 0;

	DWORD i=0;
	xmlNodePtr tmp = getRoot();
	while (tmp)
	{
		i++;
		tmp = getChild(tmp, (char *)v[i].c_str());

		if (i==v.size()-1) return tmp;
	}

	return 0;
}

bool xXMLParser::getNodeStr(xmlNodePtr node, std::string &out) const
{
    if (!node) return false;

    xmlChar *utf = xmlNodeGetContent(node);
    size_t len = (xmlStrlen(utf)+1)*2;
    char *conv = (char *) xmlMalloc(len);
    if (0==code_convert("UTF-8", "GBK", (char *)utf, xmlStrlen(utf), conv, len))
    {
        bcopy(conv, (void *)utf, (size_t)xmlStrlen(utf));
    }
    else
        XERR("xml 转码失败... %s",utf);

    out += (char *)conv;

    xmlFree(conv);
    xmlFree(utf);
    return true;
}

bool xXMLParser::getPropStr(xmlNodePtr node, const char *name, std::string &out) const
{
	if (!node) return false;
	if (!xmlHasProp(node, BAD_CAST name)) return false;

	xmlChar *utf = xmlGetProp(node, BAD_CAST name);

	//			XDBG("xml 转码前 %s", (char *)utf);
	size_t len = (xmlStrlen(utf)+1)*2;
	char *conv = (char *) xmlMalloc(len);
	if (0==code_convert("UTF-8", "GBK", (char *)utf, xmlStrlen(utf), conv, len))
	{
		//		XDBG("xml 转码后 %s size:%u", conv, xmlStrlen(utf));
		bcopy(conv, (void *)utf, (size_t)xmlStrlen(utf));
	}
	else
		XERR("xml 转码失败... %s", name);

	out += (char *)conv;
	//		std::stringstream ss((char *)conv);
	//	std::stringstream ss((char *)utf);
	//	ss>>*out;

	xmlFree(conv);
	xmlFree(utf);
	return true;
}

bool xXMLParser::getPropStr(xmlNodePtr node, const char *name, char *out, unsigned outlen) const
{
	if (!node) return false;
	if (!xmlHasProp(node, BAD_CAST name)) return false;

	xmlChar *utf = xmlGetProp(node, BAD_CAST name);

	//			XDBG("xml 转码前 %s", (char *)utf);
	size_t len = (xmlStrlen(utf)+1)*2;
	char *conv = (char *) xmlMalloc(len);
	if (0==code_convert("UTF-8", "GBK", (char *)utf, xmlStrlen(utf), conv, len))
	{
		//		XDBG("xml 转码后 %s size:%u", conv, xmlStrlen(utf));
		bcopy(conv, (void *)utf, (size_t)xmlStrlen(utf));
	}
	else
		XERR("xml 转码失败... %s", name);

	strncpy(out, (char *)conv, outlen);
	//		std::stringstream ss((char *)conv);
	//	std::stringstream ss((char *)utf);
	//	ss>>*out;

	xmlFree(conv);
	xmlFree(utf);
	return true;
}

