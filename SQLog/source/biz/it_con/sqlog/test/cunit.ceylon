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
import org.sqlite {
	SQLiteDataSource
}

void cunit() {}

"Test database creation"
test
void createDB() {
	value log = SQLog();
	log.createDB();
	//We access the database directly as there is no interface for verifying the correct initialisation of the db
	SQLiteDataSource dataSource = SQLiteDataSource();
	dataSource.url = "jdbc:sqlite:sqlog.db";
	Sql sql = Sql(newConnectionFromDataSource(dataSource));
	variable Integer records = sql.Select("SELECT COUNT(*) FROM Dictionary").singleValue<Integer>();
	assertEquals(records, 0, "Table <Dictionary> not empty");
	
	records = sql.Select("SELECT COUNT(*) FROM Log").singleValue<Integer>();
	assertEquals(records, 0, "Table <Log> not empty");
}

"Test database reset"
test
void initDB() {
	SQLog().initDB();
	//TODO Check some more, not only exception free execution ...
}

"Test the dictionary.
 Precondition: Database exists and is empty. This condition is met because of the other tests"
test
void dictionary() {
	//TODO Remove dependencies of other tests
	value log = SQLog();
	//The same entry twice must lead to the same ID
	assertEquals(log.getDictID("Test", "Ein Test-Eintrag"), 1, "First dictionary ID not assigned correctly");
	assertEquals(log.getDictID("Test", "Ein Test-Eintrag"), 1, "First dictionary ID not retrieved correctly");
	//Another entry gets a new ID
	assertEquals(log.getDictID("Test", "Noch ein Test-Eintrag"), 2, "Second dictionary ID not assigned correctly");
}

"Test the log entry.
 Create a new entry, reread it and test for equality"
test
void log() {
	value log = SQLog();
	value x = log.LogMsg();
	x.channel = "test";
	x.error = formatInteger(17);
	x.source = "irgend";
	x.text = "etwas seltsames geht hier vor";
	x.stack = ["Zeile 1", "Zeile 2", "Zeile 17", "Zeile 5", "Zeile 99"];
	
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
	value logmsg2 = log.LogMsg();
	logmsg2.load(storeID);
	//And is it equal?
	assertEquals(logmsg2, logmsg, "Retrieved log message differs from stored");
}
