"Run the module `biz.it_con.sqlog`."
shared void run() {
	//TODO command line processing
	value arg = process.arguments;
	print(arg);
	value log = SQLog("~/sqlog/sqlog");
	log.createDB();
}
