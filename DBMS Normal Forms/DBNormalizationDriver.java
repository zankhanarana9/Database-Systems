import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DBNormalizationDriver {

	public static void main(String[] args) throws Exception {
		String inputFileName = "input.txt";
		String outputFileName = "C://Users//Anudeep Krishna//Desktop//output.txt";
		String sqlDumpFile = "C://Users//Anudeep Krishna//Desktop//SQLQuery.sql";
		DBNormalization dbNormalization = new DBNormalization(inputFileName, outputFileName, sqlDumpFile);
		List<String> columnList = new ArrayList<String>();
		HashMap<String, List<String>> tableColumnMap = new HashMap<String, List<String>>();
		HashMap<String, HashMap<String, List<String>>> test = new HashMap<String, HashMap<String,List<String>>>();
		dbNormalization.resultTableGenerator();
		dbNormalization.resultSqlDump();
	}
}
