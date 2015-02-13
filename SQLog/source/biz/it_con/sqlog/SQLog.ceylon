import java.util {
	WeakHashMap
}
import java.util.concurrent.atomic { AtomicLong }

import ceylon.dbc {
	newConnectionFromDataSource,
	Sql,
	SqlNull
}
import java.sql {
	Timestamp,
	SQLException
}
import org.h2.jdbcx {
	JdbcDataSource
}
import ceylon.time {
	DateTime,
	now,
	Instant
}
import java.util.logging {
	Handler,
	LogRecord,
	Level
}
import java.lang {
	NullPointerException
}

"Unique ID for next log message"
AtomicLong logmsgID = AtomicLong(1);

"Main class. Contains the logging system."
shared class SQLog(String dbfile) {
	//TODO Should initialise db on instantiation!
	value dataSource = JdbcDataSource();
	dataSource.url = "jdbc:h2:" + dbfile;
	value sql = Sql(newConnectionFromDataSource(dataSource));
	variable WeakHashMap<[String, String], Integer> dictCache = WeakHashMap<[String, String], Integer>();
	shared variable Boolean recordStackTrace = true;
	
	"Create a new empty SQLog database or clear existing database"
	shared void createDB() {
		try {
			sql.Statement("DROP VIEW IF EXISTS LogView").execute();
			sql.Statement("DROP TABLE IF EXISTS Dictionary").execute();
			sql.Statement("DROP TABLE IF EXISTS Log").execute();
		} catch (e) {
			e.printStackTrace();
		}
		sql.Statement(
			"CREATE TABLE Dictionary (
			 ID		INTEGER IDENTITY PRIMARY KEY,
			 NAME	VARCHAR(255)	NOT NULL, 
			 TEXT	VARCHAR(4095)	NOT NULL)").execute();
		sql.Statement(
			"CREATE TABLE Log ( 
			 TIME		TIMESTAMP	NOT NULL,
			 SEQUENCE	INT	NOT NULL, 
			 CHANNEL	INT	,
			 SOURCE		INT	,
			 TEXT		INT	,
			 SEVERITY	INT	,
			 ERROR		INT	,
			 STACK		INT	,
			 CUSTOM		BLOB)").execute();
		sql.Statement(
			"CREATE INDEX LogTime on Log(TIME);
			 CREATE INDEX LogSequence on Log(SEQUENCE);
			 CREATE INDEX LogChannel on Log(CHANNEL);
			 CREATE INDEX LogSource on Log(SOURCE);
			 CREATE INDEX LogText on Log(TEXT);
			 CREATE INDEX LogSeverity on Log(SEVERITY);
			 CREATE INDEX LogError on Log(ERROR);
			 CREATE INDEX LogStack on Log(STACK);").execute();
		sql.Statement(
			"CREATE OR REPLACE VIEW LogView
			 AS
			 SELECT Log.TIME, Log.SEQUENCE, Log.SEVERITY, Log.CUSTOM, DErr.text as Error, DTxt.text as Text, 
			 	DStk.text as Stack, DChn.text as Channel, DSrc.text as Source
			 FROM Log
			 LEFT OUTER JOIN Dictionary DErr
			 ON Log.Error = DErr.ID
			 LEFT OUTER JOIN Dictionary DTxt
			 ON Log.Text = DTxt.ID
			 LEFT OUTER JOIN Dictionary DStk
			 ON Log.Stack = DStk.ID
			 LEFT OUTER JOIN Dictionary DChn
			 ON Log.Channel = DChn.ID
			 LEFT OUTER JOIN Dictionary DSrc
			 ON Log.Source = DSrc.ID"
		).execute();
		dictCache = WeakHashMap<[String, String], Integer>();
		//sql.Insert("Insert into Log values (?,?,?,?,?,?,?,?,?)")
		//		.execute(Date(),1,0,0,0,0,"Test",[1,2,3,4,5], Exception("Ein Test"));
		
	}
	
	shared void initDB() {
		variable Map<String,Object>[] maxID;
		try {
			maxID = sql.Select("SELECT MAX(Sequence) AS MAXSEQ FROM Log").execute();
		} catch (SQLException e) {
			createDB();
			return;
		}
		if (maxID.size == 0) {
			createDB();
		} else {
			Object? mID = maxID[0]?.get("maxseq");
			if (exists mID) {
				if (is Integer mID) {
					logmsgID.set(mID+1);
				} else if (is SqlNull mID) {
					createDB();
				} else {
					throw SQLogException("Unable to initialise database. Database might be corrupt.");
				}
			} else {
				createDB();
			}
		}
	}
	
	shared Handler getLogHandler() {
		object hand extends Handler() {
			value log = outer;
			shared actual void close() {}
			
			shared actual void flush() {}
			
			shared actual void publish(LogRecord? logRecord) {
				if (exists logRecord) {
					value msg = log.LogMsg();
					msg.channel = "embedded";
					msg.custom = logRecord.thrown;
					msg.error = logRecord.thrown?.message else "";
					msg.severity = logRecord.level?.intValue() else Level.\iINFO.intValue();
					msg.source = logRecord.loggerName else "java.util.logging";
					if (recordStackTrace) {
						variable StringBuilder stck = StringBuilder();
						printStackTrace(Exception(), (string) => stck.append(string));
						msg.stack=stck.string;
					}
					msg.text = logRecord.message else ""; 
					msg.timestamp = Instant(logRecord.millis).dateTime();
					msg.store();
				} else {
					throw NullPointerException("Need a log record to publish");
				}
			}
			
		}
		return hand;
	}
	
	"A log message"
	shared class LogMsg(
		timestamp = now().dateTime(), 
		channel = "", 
		source = "", 
		text = "", 
		severity = 0, 
		error = "", 
		stack = "", 
		custom = null,
		sequenceNr = -1) {
		shared variable DateTime timestamp;
		shared variable String channel;
		shared variable String source;
		shared variable String text;
		shared variable Integer severity;
		shared variable String error;
		shared variable String stack;
		shared variable Object? custom;
		
		variable Integer sequenceNr;
		shared Integer sequence => sequenceNr;
		variable Boolean stored = false;
		shared Boolean isStored => stored;
		
		shared Integer store() {
			if (stored) {throw SQLogException("Log entry is already stored");}
			sql.Insert("INSERT INTO Log VALUES (?,?,?,?,?,?,?,?,?)")
					.execute(timestamp, 
				sequenceNr, 
				getDictID("channel", channel),
				getDictID("source", source),
				getDictID("text", text),
				severity,
				getDictID("error", error),
				getDictID("stack", stack),
				custom else ""
			);
			stored = true;
			return sequenceNr;
		}
		shared actual String string =>  "``sequenceNr``/``channel``: ``timestamp`` ``text`` ``severity`` ``error``\n``stack``";
		
		void load() {
			stored = true;
			variable Map<String,Object>[] row = sql.Select(
				"SELECT * FROM LogView
				 WHERE Sequence = ?"
			).execute(sequenceNr);
			if (row.size == 0) {
				throw SQLogException("Log record ``sequence`` not found");
			} else if (row.size >1) {
				throw SQLogException("Database is corrupt: found ``row.size`` records for sequence ``sequence``");
			} else {
				value record=row[0];
				if (exists record) {
					value ts = record.get("time") else "";
					//SQLite returns Timestamp as Integer!
					if (is Integer ts) {timestamp = Instant(ts).dateTime(); }
					if (is Timestamp ts) {timestamp = Instant(ts.time).dateTime();}
					if (is DateTime ts) {timestamp = ts;}
					channel = record.get("channel")?.string else "";
					source = record.get("source")?.string else "";
					text = record.get("text")?.string else "";
					value sev = record.get("severity");
					if (is Integer sev) {severity = sev;}
					error = record.get("error")?.string else "";
					stack= record.get("stack")?.string else "";
					custom = record.get("custom");
				}
			}
		}
		
		shared actual Boolean equals(Object that) {
			if (is LogMsg that) {
				return timestamp==that.timestamp && 
						channel==that.channel && 
						source==that.source && 
						text==that.text && 
						severity==that.severity && 
						error==that.error && 
						stack==that.stack && 
						sequenceNr==that.sequenceNr && 
						stored==that.stored;
			}
			else {
				return false;
			}
		}
		
		//Initialiser
		if (sequenceNr == -1) {
			sequenceNr = logmsgID.andIncrement;
		} else {
			load();
		}
	}
	
	shared Integer newDictEntry(String name, String text) {
		value id = sql.Insert("INSERT INTO Dictionary (Name, Text) VALUES (?, ?)")
				.execute(name, text);
		value sequential = id[1];
		Object? val = sequential[0]?.get("scope_identity()");
		if (exists val) {
			if (is Integer val) {
				dictCache.put([name, text], val);
				return val;
			}
		}
		throw SQLogException("Adding value to dictionary failed. Got no ID: dict = ``name``; text=``text``");
	}
	
	shared String getDictText(String name, Integer id) {
		variable Map<String,Object>[] text = sql.Select(
			"SELECT ID 
			 FROM Dictionary
			 WHERE ID = ?
			 AND Text = ?").execute(name, id);
		if (text.size == 0) {
			return "";
		} else {
			Object? txt = text[0]?.get("text");
			if (exists txt) {
				return txt.string;
			} else {
				return "";
			}
		}
	}
	
	"Get the ID for a text string. Create new ditionary entry if needed"
	shared Integer getDictID(String name, String text) {
		value txt = String(text.sublistTo(4000));
		value cachedID = dictCache.get([name, txt]) else -1;
		if (cachedID>-1) {
			return cachedID; 
		}
		variable Map<String,Object>[] id = sql.Select(
			"SELECT ID 
			 FROM Dictionary
			 WHERE Name = ?
			 AND Text = ?").execute(name, txt);
		if (id.size == 0) {
			return newDictEntry(name, txt);
		} else {
			Object? mID = id[0]?.get("id");
			if (exists mID) {
				if (is Integer mID) {
					dictCache.put([name, txt], mID);
					return mID;
				} else {
					throw SQLogException("Got unexpected data from database. Database might be corrupt.");
				}
			} else {
				return newDictEntry(name, txt);
			}
		}
	}
}

shared 
class SQLogException(String? description = null, Throwable? cause = null)
		extends Exception(description, cause) { }