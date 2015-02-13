import ceylon.test {
	test,
	assertEquals
}
import biz.it_con.sqlog {
	SQLog,
	SQLogException
}
import ceylon.dbc {
	newConnectionFromDataSource,
	Sql
}
import org.h2.jdbcx {
	JdbcDataSource
}

import java.util {
	HashMap
}
import java.lang { 
	JStr = String,
	JInt = Integer,
	RuntimeException,
	System,
	Runtime,
	Thread
}
import ceylon.time {
	Instant
}
import java.util.logging {
	Logger,
	Level
}

SQLog log = SQLog("tcp://localhost/~/sqlog/sqlog");

void cunit() { }


"Test database creation"
test
void createDB() {
	log.createDB();
	//We access the database directly as there is no interface for verifying the correct initialisation of the db
	value dataSource = JdbcDataSource();
	dataSource.url = "jdbc:h2:tcp://localhost/~/sqlog/sqlog";
	Sql sql = Sql(newConnectionFromDataSource(dataSource));
	variable Integer records = sql.Select("SELECT COUNT(*) FROM Dictionary").singleValue<Integer>();
	assertEquals(records, 0, "Table <Dictionary> not empty");
	
	records = sql.Select("SELECT COUNT(*) FROM Log").singleValue<Integer>();
	assertEquals(records, 0, "Table <Log> not empty");
}

"Test database init"
test
void initDB() {
	log.initDB();
	value x = log.LogMsg();
	x.channel = "test";
	value storeID = x.store();
	log.initDB();
	try {
		value y = log.LogMsg(Instant(1).dateTime(),"","","",0,"","","",storeID);
		assertEquals(y, x, "Retrieved log message differs from stored");
	} catch (SQLogException sqle) {
		throw AssertionError("initDB emptied the database");
	}	
}

"Test the dictionary.
 Precondition: Database exists and is empty. This condition is met because of the other tests"
test
void dictionary() {
	//The same entry twice must lead to the same ID
	assertEquals(log.getDictID("Test", "Ein Test-Eintrag"), 1, "First dictionary ID not assigned correctly");
	assertEquals(log.getDictID("Test", "Ein Test-Eintrag"), 1, "First dictionary ID not retrieved correctly");
	//Another entry gets a new ID
	assertEquals(log.getDictID("Test", "Noch ein Test-Eintrag"), 2, "Second dictionary ID not assigned correctly");
}

"Test the log entry.
 Create a new entry, reread it and test for equality"
test
void logTest() {
	value x = log.LogMsg();
	x.channel = "test";
	x.error = formatInteger(17);
	x.source = "irgend";
	x.text = "etwas seltsames geht hier vor";
	x.stack = "Zeile 1\nZeile 2\nZeile 17\nZeile 5\nZeile 99";
	x.severity = 25;
	value t = HashMap<Object, Object>();
	x.custom = t;
	t.put(JStr("Eintrag 1"), JInt(1));
	t.put(JStr("Eintrag 2"), JStr("Wert 2"));
	t.put(JInt(12345), JInt(54321));
	
	value logmsg = x;
	//This syntax doesn't compile - although it should
	// => ceylon bug: https://github.com/ceylon/ceylon-compiler/issues/2005
	//value logmsg = log.LogMsg {
	//	channel = "test";
	//	error = formatInteger(17);
	//	source = "irgend";
	//};
	value storeID = logmsg.store();
	try {
		//The same entry may not be stored twice 
		logmsg.store();
		throw AssertionError("LogMsg could be stored twice");
	} catch (SQLogException se) {
		//All fine
	}
	//Could the stored entry be read again?
	//value logmsg2 = log.LogMsg{sequenceNr = storeID;};
	value logmsg2 = log.LogMsg(Instant(1).dateTime(),"","","",0,"","","",storeID);
	//And is it equal?
	assertEquals(logmsg2, logmsg, "Retrieved log message differs from stored");
	try {
		//The loaded entry may not be stored again 
		logmsg2.store();
		throw AssertionError("Loaded LogMsg could be stored again");
	} catch (SQLogException se) {
		//All fine
	}
	
}

"Test the interface to java.util.logging"
test
void jul() {
	log.recordStackTrace = false;
	//TODO
	value logger = Logger.anonymousLogger;
	value handlers = logger.handlers;
	for(handler in handlers.array) {logger.removeHandler(handler);}
	logger.useParentHandlers = false;
	logger.level = Level.\iALL;
	logger.addHandler(log.getLogHandler());
	logger.info("Hello world!");
	logger.finest("finest");
	for (i in 1..10) {
		logger.finest(i.string);
		for (j in 1..1000) {
			logger.severe("severe");
		}
	}
	logger.log(Level.\iINFO, "Exception", RuntimeException("Eine Exception", null));
}

