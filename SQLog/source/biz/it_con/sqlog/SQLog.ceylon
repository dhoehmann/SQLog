import java.util {
	WeakHashMap
}
import java.util.concurrent.atomic { AtomicLong }

import ceylon.dbc {
	newConnectionFromDataSource,
	Sql
}
import org.sqlite {
	SQLiteDataSource
}
import java.sql {
	Timestamp
}

"Unique ID for next log message"
AtomicLong logmsgID = AtomicLong(1);

"Main class. Contains the logging system."
shared class SQLog() {
	value dataSource = SQLiteDataSource();
	dataSource.url = "jdbc:sqlite:sqlog.db"; //TODO specify DB on command line
	value sql = Sql(newConnectionFromDataSource(dataSource));
	variable WeakHashMap<Integer,String> dictCache = WeakHashMap<Integer, String>(); //TODO use dict cache
	
	
	"Create a new empty SQLog database or clear existing database"
	shared void createDB() {
		try {
			sql.Statement("DROP TABLE Dictionary").execute();
			sql.Statement("DROP TABLE Log").execute();
		} catch (e) {}
		sql.Statement(
			"CREATE TABLE Dictionary (
			 ID		INTEGER PRIMARY KEY,
			 NAME	TEXT	NOT NULL, 
			 TEXT	TEXT	NOT NULL)").execute();
		sql.Statement(
			"CREATE TABLE Log ( 
			 TIME		TIMESTAMP	NOT NULL,
			 SEQUENCE	INT	NOT NULL, 
			 CHANNEL	INT	,
			 SOURCE		INT	,
			 TEXT		INT	,
			 SEVERITY	INT	,
			 ERROR		INT	,
			 STACK		INT[]	,
			 CUSTOM		BLOB)").execute();
		dictCache = WeakHashMap<Integer, String>();
		//sql.Insert("Insert into Log values (?,?,?,?,?,?,?,?,?)")
		//		.execute(Date(),1,0,0,0,0,"Test",[1,2,3,4,5], Exception("Ein Test"));
		
	}
	
	shared void initDB() {
		variable Map<String,Object>[] maxID = sql.Select("SELECT MAX(SEQUENCE) AS MAXSEQ FROM Log").execute();
		if (maxID.size == 0) {
			createDB(); 
		} else {
			Object? mID = maxID[0]?.get("MAXSEQ");
			if (exists mID) {
				if (is Integer mID) {
					logmsgID.set(mID+1);
				} else {
					throw SQLogException("Unable to initialise database. Database might be corrupt.");
				}
			} else {
				createDB();
			}
		}
	}
	
	"Eine Logmeldung"
	shared class LogMsg(
		timestamp = Timestamp(system.milliseconds), 
		channel = "", 
		source = "", 
		text = "", 
		severity = 0, 
		error = "", 
		stack = empty, 
		custom = null) {
		shared variable Timestamp timestamp;
		shared variable String channel;
		shared variable String source;
		shared variable String text;
		shared variable Integer severity;
		shared variable String error;
		shared variable String[] stack;
		shared variable Object? custom;
		
		variable Integer sequenceNr = logmsgID.andIncrement;
		shared Integer sequence => sequenceNr;
		variable Boolean stored = false;
		shared Boolean isStored => stored;
		
		shared Integer store() {
			if (stored) {throw SQLogException("Log entry is already stored");}
			value stackID = [for(s in stack) getDictID("stack", s)];
			sql.Insert("INSERT INTO Log VALUES (?,?,?,?,?,?,?,?,?)")
					.execute(timestamp, 
				sequenceNr, 
				getDictID("channel", channel),
				getDictID("source", source),
				getDictID("text", text),
				severity,
				getDictID("error", error),
				stackID,
				custom else ""
			);
			stored = true;
			return sequenceNr;
		}
		shared actual String string =>  "``sequenceNr``: ``timestamp`` ``text``";
		
		shared void load(Integer sequence) {
			stored = true;
			sequenceNr = sequence;
			//TODO load log message without assigning a sequence number
			//TODO dictionary
			variable Map<String,Object>[] row = sql.Select(
				"SELECT Log.*, DErr.text as Error, DTxt.text as Text
				 FROM Log
				 LEFT OUTER JOIN Dictionary DErr
				 ON Log.Error = DErr.ID
				 LEFT OUTER JOIN Dictionary DTxt
				 ON Log.Text = DTxt.ID
				 WHERE Log.Sequence = ?").execute(sequence);
			if (row.size == 0) {
				throw SQLogException("Log record ``sequence`` not found");
			} else if (row.size >1) {
				throw SQLogException("Database is corrupt: found ``row.size`` records for sequence ``sequence``");
			} else {
				value record=row[0];
				if (exists record) {
					print(record);
					//TODO timestamp = record.get("timestamp");
					channel = record.get("channel")?.string else "";
					source = record.get("source")?.string else "";
					text = record.get("text")?.string else "";
					//TODO severity
					error = record.get("error")?.string else "";
					//TODO stack;
					custom = record.get("custom");
				}
			}
		}
		
	}
	
	shared Integer newDictEntry(String name, String text) {
		value id = sql.Insert("INSERT INTO Dictionary (Name, Text) VALUES (?, ?)")
				.execute(name, text);
		value sequential = id[1];
		Object? val = sequential[0]?.get("last_insert_rowid()");
		if (exists val) {
			if (is Integer val) {
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
	
	shared Integer getDictID(String name, String text) {
		variable Map<String,Object>[] id = sql.Select(
			"SELECT ID 
			 FROM Dictionary
			 WHERE Name = ?
			 AND Text = ?").execute(name, text);
		if (id.size == 0) {
			return newDictEntry(name, text);
		} else {
			Object? mID = id[0]?.get("id");
			if (exists mID) {
				if (is Integer mID) {
					return mID;
				} else {
					throw SQLogException("Got unexpected data from database. Database might be corrupt.");
				}
			} else {
				return newDictEntry(name, text);
			}
		}
	}
}

shared 
class SQLogException(String? description = null, Throwable? cause = null)
		extends Exception(description, cause) { }