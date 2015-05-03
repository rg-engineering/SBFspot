/************************************************************************************************
	SBFspot - Yet another tool to read power production of SMA® solar inverters
	(c)2012-2014, SBF

	Latest version found at https://sbfspot.codeplex.com

	License: Attribution-NonCommercial-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
	http://creativecommons.org/licenses/by-nc-sa/3.0/

	You are free:
		to Share — to copy, distribute and transmit the work
		to Remix — to adapt the work
	Under the following conditions:
	Attribution:
		You must attribute the work in the manner specified by the author or licensor
		(but not in any way that suggests that they endorse you or your use of the work).
	Noncommercial:
		You may not use this work for commercial purposes.
	Share Alike:
		If you alter, transform, or build upon this work, you may distribute the resulting work
		only under the same or similar license to this one.

DISCLAIMER:
	A user of SBFspot software acknowledges that he or she is receiving this
	software on an "as is" basis and the user is not relying on the accuracy
	or functionality of the software for any purpose. The user further
	acknowledges that any use of this software will be at his own risk
	and the copyright owner accepts no responsibility whatsoever arising from
	the use or application of the software.

	SMA is a registered trademark of SMA Solar Technology AG

************************************************************************************************/

/***********************************************************************************************
 **                                 _   _ ____  _
 **                             ___| | | |  _ \| |
 **                            / __| | | | |_) | |
 **                           | (__| |_| |  _ <| |___
 **                            \___|\___/|_| \_\_____|
 **
 ** Curl is a command line tool for transferring data specified with URL
 ** syntax. Find out how to use curl by reading the curl.1 man page or the
 ** MANUAL document. Find out how to install Curl by reading the INSTALL
 ** document.
 **
 ** COPYRIGHT AND PERMISSION NOTICE
 **
 ** Copyright (c) 1996 - 2008, Daniel Stenberg, <daniel@haxx.se>.
 **
 ** All rights reserved.
 **
 ** Permission to use, copy, modify, and distribute this software for any purpose
 ** with or without fee is hereby granted, provided that the above copyright
 ** notice and this permission notice appear in all copies.
 **
 ** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 ** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 ** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS. IN
 ** NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 ** DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 ** OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
 ** OR OTHER DEALINGS IN THE SOFTWARE.
 **
 ** Except as contained in this notice, the name of a copyright holder shall not
 ** be used in advertising or otherwise to promote the sale, use or other dealings
 ** in this Software without prior written authorization of the copyright holder.
 **
 ***********************************************************************************************/

/***********************************************************************************************
 ** Configuration for Windows build (Visual Studio 2010)
 ** Visit http://curl.haxx.se/download.html
 **	Download latest development version for Windows (libcurl-7.18.0-win32-msvc.zip found at
 ** http://curl.haxx.se/latest.cgi?curl=win32-devel-msvc)
 **	Extract libcurl.lib to %ProgramFiles%\Microsoft Visual Studio 10.0\VC\lib
 ** Extract include\curl\*.* to %ProgramFiles%\Microsoft Visual Studio 10.0\VC\include\curl
 ** Extract libcurl.dll to %windir%\system32 or Release/Debug folder
 ** curl needs zlib1.dll - See http://sourceforge.net/projects/gnuwin32/files/zlib/1.2.3/
 ***********************************************************************************************/

/***********************************************************************************************
 ** Configuration for Linux build (Ubuntu)
 ** sudo apt-get install curl libcurl3 libcurl4-nss-dev
 ***********************************************************************************************/

#include "PVOutput.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace boost;
using namespace boost::algorithm;

PVOutput::PVOutput(unsigned int SID, std::string APIkey, unsigned int timeout)
{
	m_SID = SID;
	m_APIkey = APIkey;
	m_timeout = timeout;
	m_curlres = CURLE_OK;
	m_Donations = 0;
	m_http_header = NULL;

	/* In windows, this will init the winsock stuff */
	curl_global_init(CURL_GLOBAL_ALL);

	m_curl = curl_easy_init();

	if (m_curl)
	{
		// Create HTTP Header
		std::stringstream sid, key;
		sid << "X-Pvoutput-SystemId: " << SID;
		key << "X-Pvoutput-Apikey: " << APIkey;
		m_http_header = curl_slist_append(m_http_header, sid.str().c_str());
		m_http_header = curl_slist_append(m_http_header, key.str().c_str());
	}
	else
		m_curlres = CURLE_FAILED_INIT;
}

PVOutput::~PVOutput()
{
	curl_slist_free_all(m_http_header);
	curl_easy_cleanup(m_curl);
	curl_global_cleanup();
}

//Removed in version 3.0
//bool PVOutput::Export(Config *cfg, InverterData *inverters[])
//{
//	if (isverbose(2)) cout << "PVOutput::Export()\n";
//
//	m_curlres = CURLE_FAILED_INIT;
//
//	char postData[512];
//	const char *dt_format = "d=%Y%m%d&t=%H:%M";
//	const char *addStatus = "http://pvoutput.org/service/r2/addstatus.jsp";
//
//	//Upload highest value of Grid Voltage (Fixes issue 23)
//	long voltage = 0;
//	switch(cfg->VoltLogging)
//	{
//	case VL_NONE:	voltage = 0; break;
//	case VL_AC_MAX:	voltage = max(max(inverters[0]->Uac1, inverters[0]->Uac2), inverters[0]->Uac3); break;
//	case VL_AC_PH1:	voltage = inverters[0]->Uac1; break;
//	case VL_AC_PH2:	voltage = inverters[0]->Uac2; break;
//	case VL_AC_PH3:	voltage = inverters[0]->Uac3; break;
//	case VL_DC_MAX:	voltage = max(inverters[0]->Udc1, inverters[0]->Udc2); break;
//	case VL_DC_ST1:	voltage = inverters[0]->Udc1; break;
//	case VL_DC_ST2:	voltage = inverters[0]->Udc2; break;
//	}
//
//	if (m_curl)
//	{
//		//Calculate totals for all inverters
//	    long long sumEToday = 0;
//		long long sumETotal = 0;
//		long sumTotalPac = 0;
//		for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
//		{
//			sumEToday += inverters[inv]->EToday;
//			sumETotal += inverters[inv]->ETotal;
//			sumTotalPac += inverters[inv]->TotalPac;
//		}
//
//		if ((cfg->PVoutput_InvTempMapTo > 6) && !isSupporter())
//			cerr << "WARNING: attempting to use extended parameters (v7..v12) while not in donation mode.\n";
//
//		char vInvTemp[16]; snprintf(vInvTemp, sizeof(vInvTemp), "&v%d=%.1f", cfg->PVoutput_InvTempMapTo, (float)inverters[0]->Temperature/100);
//		char v6[16]; snprintf(v6, sizeof(v6), "&v6=%.1f", (float)voltage/100);
//
//		snprintf(postData, sizeof(postData), "%s?%s&v1=%lld&v2=%ld%s%s&c1=%d&sid=%d&key=%s",
//			addStatus, strftime_t(dt_format, inverters[0]->InverterDatetime),
//			cfg->PVoutput_CumulNRG == 0 ? sumEToday : sumETotal,
//			sumTotalPac,
//			cfg->PVoutput_InvTemp == 0 ? "" : vInvTemp,
//			voltage == 0 ? "" : v6,
//			cfg->PVoutput_CumulNRG,
//			cfg->PVoutput_SID, cfg->PVoutput_Key);
//
//	    curl_easy_setopt(m_curl, CURLOPT_URL, postData);
//		clearBuffer();
//		m_curlres = curl_easy_perform(m_curl);
//		if (isverbose(2)) cout << m_buffer << endl;
//	}
//
//	return (m_curlres == CURLE_OK);
//}


// Static Callback member function
void PVOutput::writeCallback(char *ptr, size_t size, size_t nmemb, void *f)
{
   // Call non-static member function
   static_cast<PVOutput *>(f)->writeCallback_impl(ptr, size, nmemb);
}

// Callback implementation
size_t PVOutput::writeCallback_impl(char *ptr, size_t size, size_t nmemb)
{
	m_buffer.append(ptr, size * nmemb);
	return size * nmemb;
}

CURLcode PVOutput::downloadURL(string URL)
{
	m_curlres = CURLE_FAILED_INIT;
	m_http_status = HTTP_OK;

	if (m_curl)
	{
		curl_easy_setopt(m_curl, CURLOPT_URL, URL.c_str());
		curl_easy_setopt(m_curl, CURLOPT_TIMEOUT, m_timeout);
		curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, PVOutput::writeCallback);
        curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, this);
		clearBuffer();
		m_curlres = curl_easy_perform(m_curl);
        curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &m_http_status);
	}

	return m_curlres;
}

CURLcode PVOutput::downloadURL(string URL, string data)
{
	m_curlres = CURLE_FAILED_INIT;
	m_http_status = HTTP_OK;

	if (m_curl)
	{
		curl_easy_setopt(m_curl, CURLOPT_URL, URL.c_str());
		curl_easy_setopt(m_curl, CURLOPT_POST, 1);
	    curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, data.c_str());
        curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, m_http_header);
		curl_easy_setopt(m_curl, CURLOPT_TIMEOUT, m_timeout);
		curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, PVOutput::writeCallback);
        curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, this);
		clearBuffer();
		m_curlres = curl_easy_perform(m_curl);
        curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &m_http_status);
	}

	return m_curlres;
}

CURLcode PVOutput::getSystemData(void)
{
	if (isverbose(2)) cout << "PVOutput::getSystemData()\n";

	m_curlres = CURLE_FAILED_INIT;

	if (m_curl)
	{
		if ((m_curlres = downloadURL("http://pvoutput.org/service/r2/getsystem.jsp", "teams=1&donations=1&ext=1")) == CURLE_OK)
		{
			vector<string> items;
			boost::split(items, m_buffer, boost::is_any_of(";"));
			if (items.size() == 5)
			{
				vector<string> subitems;

				// Main System data
				boost::split(subitems, items[0], boost::is_any_of(","));
				if (subitems.size() == 16)
				{
					try
					{
						m_SystemName = subitems[0];
						m_SystemSize = boost::lexical_cast<unsigned int>(subitems[1]);
						m_Postcode = subitems[2];
						m_NmbrPanels = boost::lexical_cast<unsigned int>(subitems[3]);
						m_PanelPower = boost::lexical_cast<unsigned int>(subitems[4]);
						m_PanelBrand = subitems[5];
						m_NmbrInverters = boost::lexical_cast<unsigned int>(subitems[6]);
						m_InverterPower = boost::lexical_cast<unsigned int>(subitems[7]);
						m_InverterBrand = subitems[8];
						m_Orientation = subitems[9];
						m_ArrayTilt = boost::lexical_cast<float>(subitems[10]);
						m_Shade = subitems[11];
						m_InstallDate = subitems[12];
						m_location = make_pair(boost::lexical_cast<double>(subitems[13]), boost::lexical_cast<double>(subitems[14]));
						m_StatusInterval = boost::lexical_cast<unsigned int>(subitems[15]);
					}
					catch (...)
					{
						//When we get here, it's mostly because of conversion error (boost::lexical_cast)
						if (isverbose(5)) cerr << "items[0]: " << items[0] << endl;
						m_curlres = (CURLcode) -1;
						return m_curlres;
					}
				}

				// Teams
				boost::split(subitems, items[2], boost::is_any_of(","));
				try
				{
					for (vector<string>::iterator it=subitems.begin(); it!=subitems.end(); ++it)
					{
						if (!it->empty()) m_Teams.push_back(boost::lexical_cast<unsigned int>(*it));
					}
				}
				catch (...)
				{
					if (isverbose(5)) cerr << "items[2]: " << items[2] << endl;
					m_curlres = (CURLcode) -1;
					return m_curlres;
				}

				// Donations
				try
				{
					m_Donations = boost::lexical_cast<unsigned int>(items[3]);
				}
				catch (...)
				{
					if (isverbose(5)) cerr << "items[3]: " << items[3] << endl;
					m_curlres = (CURLcode) -1;
					return m_curlres;
				}


				// Extra parameters
				boost::split(subitems, items[4], boost::is_any_of(","));
				if (subitems.size() == 12)
				{
					int v = 7;
					for (vector<string>::iterator it=subitems.begin(); it!=subitems.end(); ++it)
					{
						m_ExtData.insert(make_pair(v++, make_pair(*it, *(++it))));
					}
				}
			}
			else
			{
				if (isverbose(5)) cerr << "Received Data: " << m_buffer << endl;
				m_curlres = CURLE_URL_MALFORMAT;
			}
		}
	}

	return m_curlres;
}

bool PVOutput::isTeamMember() const
{
	const unsigned int SBFspot_TeamID = 613;
	for (vector<unsigned int>::const_iterator it=m_Teams.begin(); it!=m_Teams.end(); ++it)
	{
		if (*it == SBFspot_TeamID) return true;
	}

	return false;
}

CURLcode PVOutput::addBatchStatus(std::string data, std::string &response)
{
	if (isverbose(2)) cout << "PVOutput::addBatchStatus()\n";

	m_curlres = CURLE_FAILED_INIT;

	if (m_curl)
	{
		stringstream postData;
		postData << "c1=1&data=" << data;

		if ((m_curlres = downloadURL("http://pvoutput.org/service/r2/addbatchstatus.jsp", postData.str())) == CURLE_OK)
			response = m_buffer;
		else
			response = errtext();

	}

	return m_curlres;
}

 