/************************************************************************************************
	SBFspot - Yet another tool to read power production of SMA� solar inverters
	(c)2012-2015, SBF

	Latest version found at https://sbfspot.codeplex.com

	License: Attribution-NonCommercial-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
	http://creativecommons.org/licenses/by-nc-sa/3.0/

	You are free:
		to Share � to copy, distribute and transmit the work
		to Remix � to adapt the work
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

#include "ArchData.h"

using namespace std;
using namespace boost;
using namespace boost::date_time;
using namespace boost::posix_time;
using namespace boost::gregorian;

E_SBFSPOT ArchiveDayData(InverterData *inverters[], time_t startTime)
{
    if (VERBOSE_NORMAL)
    {
        puts("********************");
        puts("* ArchiveDayData() *");
        puts("********************");
    }

	startTime -= 86400;		// fix Issue CP23: to overcome problem with DST transition - RB@20140330

    E_SBFSPOT rc = E_OK;
    struct tm start_tm;
    memcpy(&start_tm, localtime(&startTime), sizeof(start_tm));

    start_tm.tm_hour = 0;
    start_tm.tm_min = 0;
    start_tm.tm_sec = 0;
    start_tm.tm_mday++;		// fix Issue CP23: to overcome problem with DST transition - RB@20140330
	startTime = mktime(&start_tm);

    if (VERBOSE_NORMAL)
        printf("startTime = %08lX -> %s\n", startTime, strftime_t("%d/%m/%Y %H:%M:%S", startTime));

    for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
        for(unsigned int i=0; i<sizeof(inverters[inv]->dayData)/sizeof(DayData); i++)
		{
			DayData *pdayData = &inverters[inv]->dayData[i];
            pdayData->datetime = 0;
			pdayData->totalWh = 0;
			pdayData->watt = 0;
		}


    int packetcount = 0;
    int validPcktID = 0;

    E_SBFSPOT hasData = E_ARCHNODATA;

    for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
    {
        do
        {
            pcktID++;
            writePacketHeader(pcktBuf, 0x01, inverters[inv]->BTAddress);
            writePacket(pcktBuf, 0x09, 0xE0, 0, inverters[inv]->SUSyID, inverters[inv]->Serial);
            writeLong(pcktBuf, 0x70000200);
            writeLong(pcktBuf, startTime - 300);
            writeLong(pcktBuf, startTime + 86100);
            writePacketTrailer(pcktBuf);
            writePacketLength(pcktBuf);
        }
        while (!isCrcValid(pcktBuf[packetposition-3], pcktBuf[packetposition-2]));

        if (ConnType == CT_BLUETOOTH)
            bthSend(pcktBuf);
        else
            ethSend(pcktBuf, inverters[inv]->IPAddress);

        do
        {
            unsigned long long totalWh = 0;
            unsigned long long totalWh_prev = 0;
            time_t datetime = 0;
            time_t datetime_prev = 0;
            time_t datetime_next = 0;
			bool dblrecord = false;		// Flag for double records (twins)

            const int recordsize = 12;

            do
            {
                if (ConnType == CT_BLUETOOTH)
                    rc = getPacket(inverters[inv]->BTAddress, 1);
                else
                    rc = ethGetPacket();

                if (rc != E_OK) return rc;

                packetcount = pcktBuf[25];

                //TODO: Move checksum validation to getPacket
                if ((ConnType == CT_BLUETOOTH) && (!validateChecksum()))
                    return E_CHKSUM;
                else
                {
					unsigned short rcvpcktID = get_short(pcktBuf+27) & 0x7FFF;
                    if ((validPcktID == 1) || (pcktID == rcvpcktID))
                    {
                        validPcktID = 1;
                        for(int x = 41; x < (packetposition - 3); x += recordsize)
                        {
                            datetime_next = (time_t)get_long(pcktBuf + x);
							if (0 != (datetime_next - datetime)) // Fix Issue 108: sbfspot v307 crashes for daily export (-adnn)
							{
								totalWh_prev = totalWh;
								datetime_prev = datetime;
								datetime = datetime_next;
								dblrecord = false;
							}
							else
								dblrecord = true;

							totalWh = (unsigned long long)get_longlong(pcktBuf + x + 4);
                            if (totalWh != NaN_U64) // Fix Issue 109: Bad request 400: Power value too high for system size
							{
								if (totalWh > 0) hasData = E_OK;
								if (totalWh_prev != 0)
								{
									struct tm timeinfo;
									memcpy(&timeinfo, localtime(&datetime), sizeof(timeinfo));
									if (start_tm.tm_mday == timeinfo.tm_mday)
									{
										unsigned int idx = (timeinfo.tm_hour * 12) + (timeinfo.tm_min / 5);
										if (idx < sizeof(inverters[inv]->dayData)/sizeof(DayData))
										{
											if (VERBOSE_HIGHEST && dblrecord)
											{
												std::cout << "Overwriting existing record: " << strftime_t("%d/%m/%Y %H:%M:%S", datetime);
												std::cout << " - " << std::fixed << std::setprecision(3) << (double)inverters[inv]->dayData[idx].totalWh/1000 << "kWh";
												std::cout << " - " << std::fixed << std::setprecision(0) << inverters[inv]->dayData[idx].watt << "W" << std::endl;
											}
											if (VERBOSE_HIGHEST && ((datetime - datetime_prev) > 300))
											{
												std::cout << "Missing records in datastream " << strftime_t("%d/%m/%Y %H:%M:%S", datetime_prev);
												std::cout << " -> " << strftime_t("%H:%M:%S", datetime) << std::endl;
											}
											inverters[inv]->dayData[idx].datetime = datetime;
											inverters[inv]->dayData[idx].totalWh = totalWh;
											//inverters[inv]->dayData[idx].watt = (totalWh - totalWh_prev) * 12;	// 60:5
											// Fix Issue 105 - Don't assume each interval is 5 mins
											// This is also a bug in SMA's Sunny Explorer V1.07.17 and before
											inverters[inv]->dayData[idx].watt = (totalWh - totalWh_prev) * 3600 / (datetime - datetime_prev);
										}
									}
								}
							}
                        } //for
                    }
                    else
                    {
                        if (DEBUG_HIGHEST) printf("Packet ID mismatch. Expected %d, received %d\n", pcktID, rcvpcktID);
                        validPcktID = 0;
                        packetcount = 0;
                    }
                }
            }
            while (packetcount > 0);
        }
        while (validPcktID == 0);
    }

    return hasData;
}

E_SBFSPOT ArchiveMonthData(InverterData *inverters[], tm *start_tm)
{
    if (VERBOSE_NORMAL)
    {
        puts("**********************");
        puts("* ArchiveMonthData() *");
        puts("**********************");
    }

    E_SBFSPOT rc = E_OK;

    // Set time to 1st of the month at 12:00:00
    start_tm->tm_hour = 12;
    start_tm->tm_min = 0;
    start_tm->tm_sec = 0;
    start_tm->tm_mday = 1;
    time_t startTime = mktime(start_tm);

    if (VERBOSE_NORMAL)
        printf("startTime = %08lX -> %s\n", startTime, strftime_t("%d/%m/%Y %H:%M:%S", startTime));

    for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
        for(unsigned int i=0; i<sizeof(inverters[inv]->monthData)/sizeof(MonthData); i++)
		{
            inverters[inv]->monthData[i].datetime = 0;
			inverters[inv]->monthData[i].dayWh = 0;
			inverters[inv]->monthData[i].totalWh = 0;
		}

    int packetcount = 0;
    int validPcktID = 0;

    for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
    {
        do
        {
            pcktID++;
            writePacketHeader(pcktBuf, 0x01, inverters[inv]->BTAddress);
            writePacket(pcktBuf, 0x09, 0xE0, 0, inverters[inv]->SUSyID, inverters[inv]->Serial);
            writeLong(pcktBuf, 0x70200200);
            writeLong(pcktBuf, startTime - 86400 - 86400);
            writeLong(pcktBuf, startTime + 86400 * (sizeof(inverters[inv]->monthData)/sizeof(MonthData) +1));
            writePacketTrailer(pcktBuf);
            writePacketLength(pcktBuf);
        }
        while (!isCrcValid(pcktBuf[packetposition-3], pcktBuf[packetposition-2]));

        if (ConnType == CT_BLUETOOTH)
            bthSend(pcktBuf);
        else
            ethSend(pcktBuf, inverters[inv]->IPAddress);

        do
        {
            unsigned long long totalWh = 0;
            unsigned long long totalWh_prev = 0;
            const int recordsize = 12;
            time_t datetime;

            unsigned int idx = 0;
            do
            {
                if (ConnType == CT_BLUETOOTH)
                    rc = getPacket(inverters[inv]->BTAddress, 1);
                else
                    rc = ethGetPacket();

                if (rc != E_OK) return rc;

                //TODO: Move checksum validation to getPacket
                if ((ConnType == CT_BLUETOOTH) && (!validateChecksum()))
                    return E_CHKSUM;
                else
                {
                    packetcount = pcktBuf[25];
					unsigned short rcvpcktID = get_short(pcktBuf+27) & 0x7FFF;
                    if ((validPcktID == 1) || (pcktID == rcvpcktID))
                    {
                        validPcktID = 1;

                        for(int x = 41; x < (packetposition - 3); x += recordsize)
                        {
                            datetime = (time_t)get_long(pcktBuf + x);
							//datetime -= (datetime % 86400) + 43200; // 3.0 - Round to UTC 12:00 - Removed 3.0.1 see issue C54
							datetime += inverters[inv]->monthDataOffset; // Issue 115
                            totalWh = get_longlong(pcktBuf + x + 4);
                            if (totalWh != MAXULONGLONG)
                            {
                                if (totalWh_prev != 0)
                                {
                                    struct tm utc_tm;
                                    memcpy(&utc_tm, gmtime(&datetime), sizeof(utc_tm));
                                    if (utc_tm.tm_mon == start_tm->tm_mon)
                                    {
                                        if (idx < sizeof(inverters[inv]->monthData)/sizeof(MonthData))
                                        {
											inverters[inv]->monthData[idx].datetime = datetime;
                                            inverters[inv]->monthData[idx].totalWh = totalWh;
                                            inverters[inv]->monthData[idx].dayWh = totalWh - totalWh_prev;
                                            idx++;
                                        }
                                    }
                                }
                                totalWh_prev = totalWh;
                            }
                        } //for
					}
                    else
                    {
                        if (DEBUG_HIGHEST) printf("Packet ID mismatch. Expected %d, received %d\n", pcktID, rcvpcktID);
                        validPcktID = 0;
                        packetcount = 0;
                    }
                }
            }
            while (packetcount > 0);
        }
        while (validPcktID == 0);
    }
    return E_OK;
}

E_SBFSPOT ArchiveEventData(InverterData *inverters[], boost::gregorian::date startDate, unsigned long UserGroup)
{
    E_SBFSPOT rc = E_OK;

    unsigned short pcktcount = 0;
    int validPcktID = 0;

	time_t startTime = to_time_t(startDate);
	time_t endTime = startTime + 86400 * startDate.end_of_month().day();

    for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
    {
        do
        {
            pcktID++;
            writePacketHeader(pcktBuf, 0x01, inverters[inv]->BTAddress);
            writePacket(pcktBuf, 0x09, 0xE0, 0, inverters[inv]->SUSyID, inverters[inv]->Serial);
			writeLong(pcktBuf, UserGroup == UG_USER ? 0x70100200 : 0x70120200);
			writeLong(pcktBuf, startTime);
            writeLong(pcktBuf, endTime);
            writePacketTrailer(pcktBuf);
            writePacketLength(pcktBuf);
        }
        while (!isCrcValid(pcktBuf[packetposition-3], pcktBuf[packetposition-2]));

        if (ConnType == CT_BLUETOOTH)
            bthSend(pcktBuf);
        else
            ethSend(pcktBuf, inverters[inv]->IPAddress);

		bool FIRST_EVENT_FOUND = false;
        do
        {
            do
            {
                if (ConnType == CT_BLUETOOTH)
                    rc = getPacket(inverters[inv]->BTAddress, 1);
                else
                    rc = ethGetPacket();

                if (rc != E_OK) return rc;

                //TODO: Move checksum validation to getPacket
                if ((ConnType == CT_BLUETOOTH) && (!validateChecksum()))
                    return E_CHKSUM;
                else
                {
                    pcktcount = get_short(pcktBuf+25);
					unsigned short rcvpcktID = get_short(pcktBuf+27) & 0x7FFF;
                    if ((validPcktID == 1) || (pcktID == rcvpcktID))
                    {
                        validPcktID = 1;
                        for (int x = 41; x < (packetposition - 3); x += sizeof(SMA_EVENTDATA))
                        {
							SMA_EVENTDATA *pEventData = (SMA_EVENTDATA *)(pcktBuf + x);
							if (pEventData->DateTime > 0)	// Fix Issue 89
							{
								inverters[inv]->eventData.push_back(EventData(UserGroup, pEventData));
								if (pEventData->EntryID == 1)
								{
									FIRST_EVENT_FOUND = true;
									rc = E_EOF;
								}
							}
						}

                    }
                    else
                    {
                        if (DEBUG_HIGHEST) printf("Packet ID mismatch. Expected %d, received %d\n", pcktID, rcvpcktID);
                        validPcktID = 0;
                        pcktcount = 0;
                    }
                }
            }
            while (pcktcount > 0);
        }
        while ((validPcktID == 0) && (!FIRST_EVENT_FOUND));
    }

    return rc;
}

//Issue 115
E_SBFSPOT getMonthDataOffset(InverterData *inverters[])
{
    time_t now = time(NULL);
    struct tm now_tm;
    memcpy(&now_tm, gmtime(&now), sizeof(now_tm));

	E_SBFSPOT rc = E_OK;

	rc = ArchiveMonthData(inverters, &now_tm);

	if (rc == E_OK)
	{
	    for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
		{
			inverters[inv]->monthDataOffset = 0;
			// Get last record of monthdata
		    for(unsigned int i = sizeof(inverters[inv]->monthData)/sizeof(MonthData); i > 0; i--)
			{
				if (inverters[inv]->monthData[i].datetime != 0)
				{
					if ((inverters[inv]->monthData[i].datetime - now) < 86400)
						inverters[inv]->monthDataOffset = -86400;

					break;
				}
			}

			if ((DEBUG_HIGHEST) && (!quiet))
				std::cout << "monthDataOffset=" << inverters[inv]->monthDataOffset << std::endl;
		}
	}

	return rc;
}
