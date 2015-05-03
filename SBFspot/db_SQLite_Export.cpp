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

#if defined(USE_SQLITE)

#include "db_SQLite_Export.h"

int db_SQL_Export::day_data(InverterData *inverters[])
{
	const char *sql = "INSERT INTO DayData(TimeStamp,Serial,TotalYield,Power,PVoutput) VALUES(?1,?2,?3,?4,?5)";
	int rc = SQLITE_OK;

	sqlite3_stmt* pStmt;
	if ((rc = sqlite3_prepare_v2(m_dbHandle, sql, strlen(sql), &pStmt, NULL)) == SQLITE_OK)
	{
		sqlite3_exec(m_dbHandle, "BEGIN TRANSACTION", NULL, NULL, NULL);

		for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
		{
			unsigned int first_rec, last_rec;
			for (first_rec = 0; (inverters[inv]->dayData[first_rec].watt <= 0) && (first_rec < sizeof(inverters[inv]->dayData)/sizeof(DayData)); first_rec++);
			for (last_rec = (sizeof(inverters[inv]->dayData)/sizeof(DayData))-1; (inverters[inv]->dayData[last_rec].watt <= 0) && (last_rec >= 0); last_rec--);

			for (unsigned int idx = first_rec-1; idx < last_rec+1; idx++)
			{
				// Invalid dates are not written to db
				if (inverters[inv]->dayData[idx].datetime > 0)
				{
					sqlite3_bind_int(pStmt, 1, inverters[inv]->dayData[idx].datetime);
					sqlite3_bind_int(pStmt, 2, inverters[inv]->Serial);
					sqlite3_bind_int64(pStmt, 3, inverters[inv]->dayData[idx].totalWh);
					sqlite3_bind_int64(pStmt, 4, inverters[inv]->dayData[idx].watt);
					sqlite3_bind_null(pStmt, 5);

					rc = sqlite3_step(pStmt);
					if ((rc != SQLITE_DONE) && (rc != SQLITE_CONSTRAINT))
					{
						print_error("[day_data]sqlite3_step() returned");
						break;
					}

					sqlite3_clear_bindings(pStmt);
					sqlite3_reset(pStmt);
					rc = SQLITE_OK;
				}
			}
		}

		sqlite3_finalize(pStmt);

		if (rc == SQLITE_OK)
			sqlite3_exec(m_dbHandle, "COMMIT", NULL, NULL, NULL);
		else
			sqlite3_exec(m_dbHandle, "ROLLBACK", NULL, NULL, NULL);
	}

	return rc;
}

int db_SQL_Export::month_data(InverterData *inverters[])
{
	const char *sql = "INSERT INTO MonthData(TimeStamp,Serial,TotalYield,DayYield) VALUES(?1,?2,?3,?4)";
	int rc = SQLITE_OK;

	sqlite3_stmt* pStmt;
	if ((rc = sqlite3_prepare_v2(m_dbHandle, sql, strlen(sql), &pStmt, NULL)) == SQLITE_OK)
	{
		sqlite3_exec(m_dbHandle, "BEGIN TRANSACTION", NULL, NULL, NULL);

		for (int inv=0; inverters[inv]!=NULL && inv<MAX_INVERTERS; inv++)
		{
			//Fix Issue 74: Double data in Monthdata tables
			tm *ptm = gmtime(&inverters[inv]->monthData[0].datetime);
			char dt[32];
			strftime(dt, sizeof(dt), "%Y-%m", ptm);  

			std::stringstream rmvsql;
			rmvsql.str("");
			rmvsql << "DELETE FROM MonthData WHERE Serial=" << inverters[inv]->Serial << " AND strftime('%Y-%m',datetime(TimeStamp, 'unixepoch'))='" << dt << "';";

			rc = sqlite3_exec(m_dbHandle, rmvsql.str().c_str(), NULL, NULL, NULL);
			if (rc != SQLITE_OK)
			{
				print_error("[month_data]sqlite3_exec() returned");
				break;
			}

			for (unsigned int idx = 0; idx < sizeof(inverters[inv]->monthData)/sizeof(MonthData); idx++)
			{
				if (inverters[inv]->monthData[idx].datetime > 0)
				{
					sqlite3_bind_int(pStmt, 1, inverters[inv]->monthData[idx].datetime);
					sqlite3_bind_int(pStmt, 2, inverters[inv]->Serial);
					sqlite3_bind_int64(pStmt, 3, inverters[inv]->monthData[idx].totalWh);
					sqlite3_bind_int64(pStmt, 4, inverters[inv]->monthData[idx].dayWh);

					rc = sqlite3_step(pStmt);
					if ((rc != SQLITE_DONE) && (rc != SQLITE_CONSTRAINT))
					{
						print_error("[month_data]sqlite3_step() returned");
						break;
					}

					sqlite3_clear_bindings(pStmt);
					sqlite3_reset(pStmt);
					rc = SQLITE_OK;
				}
			}
		}

		sqlite3_finalize(pStmt);

		if (rc == SQLITE_OK)
			sqlite3_exec(m_dbHandle, "COMMIT", NULL, NULL, NULL);
		else
			sqlite3_exec(m_dbHandle, "ROLLBACK", NULL, NULL, NULL);
	}

	return rc;
}

int db_SQL_Export::spot_data(InverterData *inv[], time_t spottime)
{
	std::stringstream sql;
	int rc = SQLITE_OK;

	for (int i=0; inv[i]!=NULL && i<MAX_INVERTERS; i++)
	{
		sql.str("");
		sql << "INSERT INTO SpotData VALUES(" <<
		spottime << ',' <<
		inv[i]->Serial << ',' <<
		inv[i]->Pdc1 << ',' <<
		inv[i]->Pdc2 << ',' <<
		(float)inv[i]->Idc1/1000 << ',' <<
		(float)inv[i]->Idc2/1000 << ',' <<
		(float)inv[i]->Udc1/100 << ',' <<
		(float)inv[i]->Udc2/100 << ',' <<
		inv[i]->Pac1 << ',' <<
		inv[i]->Pac2 << ',' <<
		inv[i]->Pac3 << ',' <<
		(float)inv[i]->Iac1/1000 << ',' <<
		(float)inv[i]->Iac2/1000 << ',' <<
		(float)inv[i]->Iac3/1000 << ',' <<
		(float)inv[i]->Uac1/100 << ',' <<
		(float)inv[i]->Uac2/100 << ',' <<
		(float)inv[i]->Uac3/100 << ',' <<
		inv[i]->EToday << ',' <<
		inv[i]->ETotal << ',' <<
		(float)inv[i]->GridFreq/100 << ',' <<
		(double)inv[i]->OperationTime/3600 << ',' <<
		(double)inv[i]->FeedInTime/3600 << ',' <<
		(float)inv[i]->BT_Signal << ',' <<
		s_quoted(status_text(inv[i]->DeviceStatus)) << ',' <<
		s_quoted(status_text(inv[i]->GridRelayStatus)) << ',' <<
		(float)inv[i]->Temperature/100 << ")";

		if ((rc = exec_query(sql.str())) != SQLITE_OK)
		{
			print_error("[spot_data]exec_query() returned", sql.str());
			break;
		}
	}

	return rc;
}

int db_SQL_Export::event_data(InverterData *inv[], TagDefs& tags)
{
	const char *sql = "INSERT INTO EventData(EntryID,TimeStamp,Serial,SusyID,EventCode,EventType,Category,EventGroup,Tag,OldValue,NewValue,UserGroup) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)";
	int rc = SQLITE_OK;

	sqlite3_stmt* pStmt;
	if ((rc = sqlite3_prepare_v2(m_dbHandle, sql, strlen(sql), &pStmt, NULL)) == SQLITE_OK)
	{
		sqlite3_exec(m_dbHandle, "BEGIN TRANSACTION", NULL, NULL, NULL);

		for (int i=0; inv[i]!=NULL && i<MAX_INVERTERS; i++)
		{
			for (std::vector<EventData>::iterator it=inv[i]->eventData.begin(); it!=inv[i]->eventData.end(); ++it)
			{
				std::string grp = tags.getDesc(it->Group());
				std::string tag = tags.getDesc(it->Tag());

				// If description contains "%s", replace it with localized parameter
				size_t start_pos = tag.find("%s");
				if (start_pos != std::string::npos)
					tag.replace(start_pos, 2, tags.getDescForLRI(it->Parameter()));

				std::string usrgrp = tags.getDesc(it->UserGroupTagID());
				std::stringstream oldval;
				std::stringstream newval;

				switch (it->DataType())
				{
					case DT_STATUS:
						oldval << tags.getDesc(it->OldVal() & 0xFFFF);
						newval << tags.getDesc(it->NewVal() & 0xFFFF);
						break;

					case DT_STRING:
						oldval.width(8); oldval.fill('0');
						oldval << it->OldVal();
						newval.width(8); newval.fill('0');
						newval << it->NewVal();
						break;

					default:
						oldval << it->OldVal();
						newval << it->NewVal();
				}

				sqlite3_bind_int(pStmt,  1, it->EntryID());
				sqlite3_bind_int(pStmt,  2, it->DateTime());
				sqlite3_bind_int(pStmt,  3, it->SerNo());
				sqlite3_bind_int(pStmt,  4, it->SUSyID());
				sqlite3_bind_int(pStmt,  5, it->EventCode());
				sqlite3_bind_text(pStmt, 6, it->EventType().c_str(), it->EventType().size(), SQLITE_TRANSIENT);
				sqlite3_bind_text(pStmt, 7, it->EventCategory().c_str(), it->EventCategory().size(), SQLITE_TRANSIENT);
				sqlite3_bind_text(pStmt, 8, grp.c_str(), grp.size(), SQLITE_TRANSIENT);
				sqlite3_bind_text(pStmt, 9, tag.c_str(), tag.size(), SQLITE_TRANSIENT);
				sqlite3_bind_text(pStmt,10, oldval.str().c_str(), oldval.str().size(), SQLITE_TRANSIENT);
				sqlite3_bind_text(pStmt,11, newval.str().c_str(), newval.str().size(), SQLITE_TRANSIENT);
				sqlite3_bind_text(pStmt,12, usrgrp.c_str(), usrgrp.size(), SQLITE_TRANSIENT);

				rc = sqlite3_step(pStmt);
				if ((rc != SQLITE_DONE) && (rc != SQLITE_CONSTRAINT))
				{
					print_error("[event_data]sqlite3_step() returned");
					break;
				}

				sqlite3_clear_bindings(pStmt);
				sqlite3_reset(pStmt);
				rc = SQLITE_OK;
			} //for
		}

		sqlite3_finalize(pStmt);

		if (rc == SQLITE_OK)
			rc = sqlite3_exec(m_dbHandle, "COMMIT", NULL, NULL, NULL);
		else
			rc = sqlite3_exec(m_dbHandle, "ROLLBACK", NULL, NULL, NULL);
	}

	return rc;
}

#endif
