import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class DBNormalization {
	public static String colName, tableName;
	public static Map<String, List<String>> tableColumnMap;
	public static Map<String, HashMap<String, List<String>>> tableColKeyMap;
	public static List<String> colList, keysList, nonKeysList;
	public static StringBuilder decompDetails = new StringBuilder("\n");
	public static String result = "team05schema.NF_TEMP_TEAM5";
	public static int columnFlag = 0;
	public static StringBuilder sqlDumpString = new StringBuilder("\n");
	public static String SQLFile, inputFile;
	public static String outputFile;
	Map<String, HashMap<String, List<String>>> tableTestMap;

	public DBNormalization(String inputFile, String outputFile, String SQLFile) {
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.SQLFile = SQLFile;
		colList = new ArrayList<String>();
		keysList = new ArrayList<String>();
		nonKeysList = new ArrayList<String>();
		tableColKeyMap = new HashMap<String, HashMap<String, List<String>>>();
		tableColumnMap = new HashMap<String, List<String>>();
		tableTestMap = new HashMap<String, HashMap<String, List<String>>>();
		init();
		verify(tableColKeyMap);
	}

	private void init() {
        tableColKeyMap = new HashMap<String, HashMap<String, List<String>>>();
        HashMap<String, List<String>> map;
        try {
            List<String> lines = Files.readAllLines(Paths.get(inputFile));
            for (String line : lines) {
                map = new HashMap<String, List<String>>();
                keysList = new ArrayList<>();
                nonKeysList = new ArrayList<>();
                tableName = line.substring(0, line.indexOf("("));
                colName = line.substring(line.indexOf("(") + 1, line.length() - 1);
                String[] strArr = colName.split(",");
                for (String str : strArr) {
                    if (str.contains("(")) {
                        keysList.add(str.toString().substring(0, str.indexOf("(")));
                    } else {
                        nonKeysList.add(str.toString());
                    }
                }
                map.put("Key", keysList);
                map.put("NonKey", nonKeysList);
                tableColKeyMap.put(tableName, map);
            }
            Connection con = getConnection();
            Statement st = null;
            try {
                st = con.createStatement();
                st.execute("DROP TABLE IF EXISTS " + result);
                st.execute("CREATE TABLE " + result
                        + "(Name varchar(200), NormalForm varchar(5),isInNormalForm varchar(3),WhyNotInNormalForm varchar(200))");
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public static Connection getConnection() {
		Connection con = null;
		try {
			Class.forName("com.vertica.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Could not find the JDBC driver class.");
			e.printStackTrace();
		}
		Properties myProp = new Properties();
		myProp.put("user", "team05");
		myProp.put("password", "874K2nan");
		try {
			con = DriverManager.getConnection("jdbc:vertica://129.7.243.243:5433/cosc6340s17", myProp);
		} catch (SQLException e) {
			// Could not connect to database.
			System.out.println("Could not connect to database.");
			e.printStackTrace();
		}
		return con;
	}
	
	public int checkTableExists(String tableName) throws Exception {
		Statement st = null;
		Connection con = getConnection();
		int retVal = 0;
		try {
			st = con.createStatement();
			String strSmt = "SELECT count(table_name) as cnt FROM v_catalog.tables where table_name='" + tableName
					+ "'";
			ResultSet rs = st.executeQuery(strSmt);
			sqlDumpString.append(strSmt);
			sqlDumpString.append("\n");
			while (rs.next()) {
				retVal = rs.getInt(1);
			}
			rs.close();
			st.close();
			con.close();
		} catch (SQLException e) {
			System.out.println("Could not create statement");
			e.printStackTrace();
		}

		return retVal;

	}

	// check if Column exists
	public int checkColumnExist(String tableName, List<String> columnNameList) {
		System.out.println(tableName + columnNameList);
		Statement st = null;
		Statement st1 = null;
		Connection conn = getConnection();
		int retVal = 0;
		try {
			st = conn.createStatement();
			for (String columnName : columnNameList) {
				String strSmt = "SELECT count(column_name) as cnt FROM v_catalog.columns where table_name='" + tableName
						+ "' and column_name ='" + columnName + "'";
				ResultSet rs1 = st.executeQuery(strSmt);
				sqlDumpString.append(strSmt);
				sqlDumpString.append("\n");
				while (rs1.next()) {
					if (rs1.getInt("cnt") == 1) {
						retVal = 1;
					} else {
						st1 = conn.createStatement();
						String tblNtExist = "INSERT INTO " + result + " VALUES ('" + tableName + "', '"
								+ columnName + "', '', 'COLUMN DOES NOT EXIST')";
						st1.executeUpdate(tblNtExist);
						sqlDumpString.append(tblNtExist);
						sqlDumpString.append("\n");
						st1.close();
					}
				}
			}
			st.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}

	public void verify(Map<String, HashMap<String, List<String>>> tableColumnKeyMap) {
		Connection con = getConnection();
		HashMap<String, List<String>> map = new HashMap<String, List<String>>();
		List<String> colList;
		Statement st = null;
		Statement st1 = null;
		List<String> keysList;
		List<String> nonKeysList;
		String queryOne = "", queryTwo = "";
		int tableFlag = 0, nullCount = 0;
		String isInNormalForm = "", NF_KEY = "", NForm1 = "";
		String firstNormalFormQuery = "", secondNormalFormQuery = "";
		int isInFirstNormalForm = 0, isInSecondNormalForm = 0;
		String thirdNormalFormQuery = "", thirdNormalFormQueryTwo = "";

		try {
			for (String tblName : tableColumnKeyMap.keySet()) {
				colList = new ArrayList<String>();
				map = tableColumnKeyMap.get(tblName);
				keysList = new ArrayList<String>();
				nonKeysList = new ArrayList<String>();
				keysList = map.get("Key");
				nonKeysList = map.get("NonKey");
				colList.addAll(nonKeysList);
				colList.addAll(keysList);
				if (keysList.size() > 1) {
					// Null Check
					queryOne = "SELECT COUNT(*) FROM ( SELECT " + keysList.get(0) + "," + keysList.get(1) + " FROM ("
							+ "SELECT * FROM " + tblName + " WHERE (" + keysList.get(0) + "," + keysList.get(1)
							+ ") IS NULL)T" + " GROUP BY " + keysList.get(0) + "," + keysList.get(1) + " ) TE";

					firstNormalFormQuery = "SELECT COUNT(*) FROM " + tblName;
					secondNormalFormQuery = "SELECT COUNT(*) FROM ( SELECT " + keysList.get(0) + "," + keysList.get(1)
							+ " FROM ( SELECT * FROM " + tblName + " WHERE (" + keysList.get(0) + "," + keysList.get(1)
							+ ") IS NOT NULL)T GROUP BY " + keysList.get(0) + "," + keysList.get(1)
							+ " HAVING COUNT(*) = 1) as TE";

					queryTwo = "SELECT COUNT(*) FROM (SELECT " + keysList.get(0)
							+ " FROM " + tblName + " WHERE " + keysList.get(0)+" OR "+keysList.get(1)
							+ " IS NULL )TE) > 0";// THEN 'KEY1' ELSE 'KEY2' END AS NF_KEY";
				} else {
					// Null Check
					queryOne = "SELECT COUNT(*) FROM ( SELECT " + keysList.get(0) + " FROM (" + "SELECT * FROM " + tblName
							+ " WHERE " + keysList.get(0) + " IS NULL)T" + " GROUP BY " + keysList.get(0) + " ) TE";

					firstNormalFormQuery = "SELECT COUNT(*) FROM " + tblName;
					secondNormalFormQuery = "SELECT COUNT(*) FROM (SELECT " + keysList.get(0) + " FROM ( SELECT * FROM " + tblName
							+ " WHERE " + keysList.get(0) + " IS NOT NULL) AS T GROUP BY T." + keysList.get(0)
							+ " HAVING COUNT(*) = 1) as TE";

					queryTwo = "SELECT COUNT(*) AS CNT FROM (SELECT " + keysList.get(0)
							+ " FROM " + tblName + " WHERE " + keysList.get(0)
							+ " IS NULL )TE) > 0 ";//THEN 'KEY1' ELSE 'KEY2' END AS NF_KEY";

				}
				// check if table exist
				tableFlag = checkTableExists(tblName);

				if (tableFlag == 0) {
					st = con.createStatement();
					String tblNtExist = "INSERT INTO " + result + " VALUES ('" + tblName
							+ "', '', '', 'TABLE DOES NOT EXIST')";
					st.executeQuery(tblNtExist);
					sqlDumpString.append(tblNtExist);
					sqlDumpString.append("\n");
					st.close();
				} else {
					columnFlag = checkColumnExist(tblName, colList);
					if (columnFlag != 0) {
						st1 = con.createStatement();
						ResultSet rs1 = st1.executeQuery(queryOne);
						sqlDumpString.append(queryOne);
						sqlDumpString.append("\n");
						while (rs1.next()) {
							nullCount = rs1.getInt(1);
						}
						if (nullCount > 0) {
							rs1 = st1.executeQuery(queryTwo);
							sqlDumpString.append(queryTwo);
							sqlDumpString.append("\n");
							while (rs1.next()) {
								int f=rs1.getInt(1);
								if(f>0)
									NF_KEY="KEY1";
								else
									NF_KEY="KEY2";
							}
							if (NF_KEY.equals("KEY1"))
								st1.execute("INSERT INTO  " + result + "  VALUES ('" + tblName
										+ "', '1NF', 'N', '" + keysList.get(0) + " HAS NULL VALUES')");
							else
								st.execute("INSERT INTO  " + result + "  VALUES ('" + tblName + "', '1NF', 'N', '"
										+ keysList.get(1) + " HAS NULL VALUES')");
						} else {
							if (keysList.size() > 1) {
								ResultSet rs_1 = st1.executeQuery(firstNormalFormQuery);
								sqlDumpString.append(firstNormalFormQuery);
								sqlDumpString.append("\n");
								while (rs_1.next()) {
									isInFirstNormalForm = rs_1.getInt(1);
								}
								ResultSet rs_2 = st1.executeQuery(secondNormalFormQuery);
								sqlDumpString.append(secondNormalFormQuery);
								sqlDumpString.append("\n");
								while (rs_2.next()) {
									isInSecondNormalForm = rs_2.getInt(1);
								}
								Statement st2 = null;
								st2 = con.createStatement();
								if (isInFirstNormalForm == isInSecondNormalForm)
									st2.execute("INSERT INTO  " + result + "  VALUES ('" + tblName
											+ "', '1NF', 'Y', '')");
								else
									st2.execute("INSERT INTO  " + result + "  VALUES ('" + tblName
											+ "', '1NF', 'N', '" + keysList.get(0) + "," + keysList.get(1)
											+ " COMBINATION HAS DUPLICATE VALUES')");
							} else {
								ResultSet rs_1 = st1.executeQuery(firstNormalFormQuery);
								sqlDumpString.append(firstNormalFormQuery);
								sqlDumpString.append("\n");
								while (rs_1.next()) {
									isInFirstNormalForm = rs_1.getInt(1);
								}
								ResultSet rs_2 = st1.executeQuery(secondNormalFormQuery);
								sqlDumpString.append(secondNormalFormQuery);
								sqlDumpString.append("\n");
								while (rs_2.next()) {
									isInSecondNormalForm = rs_2.getInt(1);
								}
								Statement st2 = null;
								st2 = con.createStatement();
								if (isInFirstNormalForm == isInSecondNormalForm)
									st2.execute("INSERT INTO  " + result + "  VALUES ('" + tblName
											+ "', '1NF', 'Y', '')");
								else
									st2.execute("INSERT INTO  " + result + "  VALUES ('" + tblName
											+ "', '1NF', 'N', '" + keysList.get(0) + " HAS DUPLICATE VALUES')");
							}

						}
						String selectqry="SELECT isInNormalForm FROM  " + result + "  WHERE NAME = '" + tblName
								+ "' AND NORMALFORM = '1NF'";
						rs1 = st1.executeQuery(selectqry);
						sqlDumpString.append(selectqry);
						sqlDumpString.append("\n");
						while (rs1.next()) {
							isInNormalForm = rs1.getString("isInNormalForm");
						}
						Statement st_1_2 = null;
						st_1_2 = con.createStatement();
						if (isInNormalForm.equals("Y")) {
							String insertqry = "INSERT INTO  " + result + "  VALUES ('" + tblName
									+ "', '2NF', '', '')";
							st_1_2.execute(insertqry);
							if (keysList.size() == 1){
							String updtqry="UPDATE  " + result + "  SET isInNormalForm = 'Y' WHERE NAME = '"
									+ tblName + "' AND NORMALFORM = '2NF'";
								st_1_2.execute(updtqry);
							sqlDumpString.append(updtqry);
							sqlDumpString.append("\n");
							}else {
								List<String> ck_FD = new ArrayList<String>();
								List<String> nck_FD = new ArrayList<String>();
								for (int i = 0; i < keysList.size(); i++) {
									for (int j = 0; j < nonKeysList.size(); j++) {
										String NF2_1 = "SELECT COUNT(*) FROM (SELECT t." + keysList.get(i) + ",t."
												+ nonKeysList.get(j) + " FROM " + tblName + " AS t INNER JOIN ( SELECT "
												+ keysList.get(i) + " FROM " + tblName + " GROUP BY " + keysList.get(i)
												+ " HAVING COUNT( DISTINCT " + nonKeysList.get(j) + ") > 1 ) g ON t."
												+ keysList.get(i) + " = g." + keysList.get(i) + " WHERE t."
												+ nonKeysList.get(j) + " IS NOT NULL GROUP BY t." + keysList.get(i)
												+ ", t." + nonKeysList.get(j) + " ) X";
										Statement stnf2 = null;
										stnf2 = con.createStatement();
										ResultSet rsnf2 = stnf2.executeQuery(NF2_1);
										sqlDumpString.append(NF2_1);
										sqlDumpString.append("\n");
										int nf2_flag = 1;
										while (rsnf2.next()) {
											nf2_flag = rsnf2.getInt(1);
										}
										if (nf2_flag == 0) {
											ck_FD.add(keysList.get(i));
											nck_FD.add(nonKeysList.get(j));
										}
										stnf2.close();
									}
								}
								if (ck_FD.size() > 0){
									Statement stnf21 = null;
									stnf21 = con.createStatement();
									String printStr = "";
									for (int i = 0; i<ck_FD.size();i++){
										printStr = printStr + ck_FD.get(i) + "->" + nck_FD.get(i)+ " ";
									}
									String updatequr = "UPDATE  " + result + "  SET WhyNotInNormalForm ='"
											+ printStr
											+ "' , isInNormalForm = 'N' WHERE NAME ='" + tblName
											+ "' AND NORMALFORM = '2NF' ";
									stnf21.executeUpdate(updatequr);
									sqlDumpString.append(updatequr);
									sqlDumpString.append("\n");
									decompDetails.append("2NF Decomposition");
									decompDetails.append("\n");
									System.out.println("2NF Decomposition");
									// We have FD. Let's decompose
									//System.out.println(ck_FD);
									//System.out.println(nck_FD);
									List<String> Decomp_ck = new ArrayList<String>();
									List<String> Decomp_nck = new ArrayList<String>();
									List<Integer> allNCKeys = new ArrayList<Integer>(); // Will contain 1 if the non candidate key is not deleted. 0 if deleted. 
									for (int i=0;i<nonKeysList.size();i++){
										allNCKeys.add(1);
									}
									
									int sum;
									String Relation;
									int count = 0;
									int flag = 1;
									for (int i = 0; i<keysList.size(); i++){// For every candidate key
										for (int j = 0; j<nonKeysList.size(); j++){ // For every non candidate key
											sum = allNCKeys.stream().mapToInt(Integer::intValue).sum(); // If we haven't finished with all non-candidate keys
											if (sum==0){
												break;
											}
											if (allNCKeys.get(j) == 1){ // If we have not deleted the non-candidate key 
												for (int z = 0; z<ck_FD.size(); z++){ // Check if the FD between the key and the non-candidate key exists
													if (keysList.get(i) == ck_FD.get(z) & nonKeysList.get(j) == nck_FD.get(z)){
														Decomp_ck.add(ck_FD.get(z));
														Decomp_nck.add(nck_FD.get(z));
														count++;
														Relation = "R"+ count + "(" + ck_FD.get(z)+"," + nck_FD.get(z) +")";
														decompDetails.append(Relation);
														decompDetails.append("\n");
														System.out.println(Relation);
														allNCKeys.set(j, Integer.valueOf(0));
														break;
													}
												}
											}
										}
										sum = allNCKeys.stream().mapToInt(Integer::intValue).sum();
										if (sum==0){
											flag = 0;
											break;
										}
									}
									count++;
									if (flag==0){
										// PRINT ALL CANDIDATE KEYS
										String pr = "R" + count +"(";
										String conc = "(";
										for (int i=0;i<keysList.size();i++){
											if (i == keysList.size()-1){
												pr = pr + keysList.get(i)+")";
												conc = conc + keysList.get(i) +")";
											}else{
												conc = conc+ keysList.get(i)+",";
												pr = pr + keysList.get(i)+",";
											}
											
										}
										Decomp_ck.add(conc);
										Decomp_nck.add(" ");
										System.out.println(pr);
										decompDetails.append(pr);
										decompDetails.append("\n");
										//writeContent(content.toString());
										//System.out.println(Decomp_ck);
										//System.out.println(Decomp_nck);
									}else{
										sum = allNCKeys.stream().mapToInt(Integer::intValue).sum();
										if (sum>1){
											for (int i = 0; i<keysList.size(); i++){
												for (int j = 0; j<nonKeysList.size(); j++){
													// Printing for more than 1 candidate keys and more than one non-canidadate with a FD
												}
												
											}
										}else{
											// Print all candidate keys and whatever's left from the non-candidate
											String pr = "R" + count +"(";
											for (int i=0;i<keysList.size();i++){
												pr = pr + keysList.get(i)+",";
											}
											for (int i=0;i<allNCKeys.size();i++){
												sum = allNCKeys.stream().mapToInt(Integer::intValue).sum();
												if (allNCKeys.get(i)==1 & sum >1){
													pr = pr + nonKeysList.get(i)+",";
													allNCKeys.set(i, Integer.valueOf(0));
												}else if (allNCKeys.get(i)==1 & sum ==1){
													pr = pr + nonKeysList.get(i)+")";
													allNCKeys.set(i, Integer.valueOf(0));
												}
											}
											decompDetails.append(pr);
											decompDetails.append("\n");
											System.out.println(pr);					
										}
									}
									//Join Verification
									
									//String J = "SELECT DISTINCT " + Decomp_ck.get(0) + "," + Decomp_nck.get(0) + " INTO Join1 FROM " + tblName ;
									//String J1 = "SELECT * FROM " + "Join1";
									//String Join = "SELECT DISTINCT " + Decomp_ck.get(0) + "," + Decomp_nck.get(0) + " FROM " + tblName ;
									//Statement stnfJ = null;
									//stnfJ = con.createStatement();
									//stnfJ.execute("DROP TABLE IF EXISTS" +" Join1");
									// stnf2.execute(NF2_1);
									//stnfJ.execute(J);
									//ResultSet rsnf2 = stnfJ.executeQuery(J1);
									//while(rsnf2.next()){
										
									//}
									//sqlDumpString.append(J);
									//sqlDumpString.append("\n");
								}
							}
							String rsltqry = "SELECT isInNormalForm FROM  " + result + "  WHERE NAME = '" + tblName
									+ "' AND NORMALFORM = '2NF'";
							rs1 = st1.executeQuery(rsltqry);
							sqlDumpString.append(rsltqry);
							sqlDumpString.append("\n");
							while (rs1.next()) {
								NForm1 = rs1.getString("isInNormalForm");
							}
							if ((NForm1.equals("")) || NForm1.equals("Y")) {
								String updateqry="UPDATE  " + result + "  SET isInNormalForm = 'Y' WHERE NAME = '" + tblName
										+ "' AND NORMALFORM = '2NF'";
								st1.execute(updateqry);
								sqlDumpString.append(updateqry);
								sqlDumpString.append("\n");						
								// checking for 3NF
								List<String> ck_FD_3NF = new ArrayList<String>();
								List<String> nck_FD_3NF = new ArrayList<String>();
								st1.execute("INSERT INTO  " + result + "  VALUES ('" + tblName + "', '3NF', '', '')");
								for (int i = 0; i < (nonKeysList.size() - 1); i++) {
									for (int j = (i + 1); j < nonKeysList.size(); j++) {

										thirdNormalFormQuery = "SELECT COUNT(*) FROM (SELECT t." + nonKeysList.get(i) + ",t."
												+ nonKeysList.get(j) + " FROM " + tblName + " AS t INNER JOIN ( SELECT "
												+ nonKeysList.get(i) + " FROM " + tblName + " GROUP BY "
												+ nonKeysList.get(i) + " HAVING COUNT( DISTINCT " + nonKeysList.get(j)
												+ ") > 1 ) g ON t." + nonKeysList.get(i) + " = g." + nonKeysList.get(i)
												+ " WHERE (t." + nonKeysList.get(i) + " , t." + nonKeysList.get(j)
												+ ") IS NOT NULL GROUP BY t." + nonKeysList.get(i) + ", t."
												+ nonKeysList.get(j) + " ) X ";

										Statement stnf3 = null;
										stnf3 = con.createStatement();
										ResultSet rsnf3 = stnf3.executeQuery(thirdNormalFormQuery);
										sqlDumpString.append(thirdNormalFormQuery);
										sqlDumpString.append("\n");	
										int nf3_flag = 1;
										while (rsnf3.next()) {
											nf3_flag = rsnf3.getInt(1);
										}
										if (nf3_flag == 0) {
											ck_FD_3NF.add(nonKeysList.get(i));
											nck_FD_3NF.add(nonKeysList.get(j)); // save can keys and non can keys that are func. dependent
											//String updatequr = "UPDATE " + result + "  SET WhyNotInNormalForm = '"
											//		+ colNKList.get(i) + "->" + colNKList.get(j)
											//		+ "  ' , isInNormalForm = 'N' WHERE NAME='" + tblName
											//		+ "' AND NORMALFORM='3NF' ";
											//stnf3.executeUpdate(updatequr);
											//sqlDumpString.append(updatequr);
											//sqlDumpString.append("\n");	
										}
										stnf3.close();
										thirdNormalFormQueryTwo = "SELECT COUNT(*) FROM (SELECT t." + nonKeysList.get(j) + ",t."
												+ nonKeysList.get(i) + " FROM " + tblName + " AS t INNER JOIN ( SELECT "
												+ nonKeysList.get(j) + " FROM " + tblName + " GROUP BY "
												+ nonKeysList.get(j) + " HAVING COUNT( DISTINCT " + nonKeysList.get(i)
												+ ") > 1 ) g ON t." + nonKeysList.get(j) + " = g." + nonKeysList.get(j)
												+ " WHERE (t." + nonKeysList.get(j) + " , t." + nonKeysList.get(i)
												+ ") IS NOT NULL GROUP BY t." + nonKeysList.get(j) + ", t."
												+ nonKeysList.get(i) + " ) X ";

										Statement stnf3_1 = null;
										stnf3_1 = con.createStatement();
										ResultSet rsnf3_1 = stnf3_1.executeQuery(thirdNormalFormQueryTwo);
										sqlDumpString.append(thirdNormalFormQueryTwo);
										sqlDumpString.append("\n");	
										nf3_flag = 1;
										while (rsnf3_1.next()) {
											nf3_flag = rsnf3_1.getInt(1);
										}
										if (nf3_flag == 0) {
											//ck_FD_3NF.add(colNKList.get(j));
											//nck_FD_3NF.add(colNKList.get(i));
											//String updatequr = "UPDATE " + result + "  SET WhyNotInNormalForm = '"
											//		+ colNKList.get(j) + "->" + colNKList.get(i)
											//		+ "', isInNormalForm = 'N' WHERE NAME='" + tblName
											//		+ "' AND NORMALFORM='3NF' ";
											//stnf3_1.executeUpdate(updatequr);
											//sqlDumpString.append(updatequr);
											//sqlDumpString.append("\n");	
											
										}
										stnf3_1.close();
									}
								}
								if (ck_FD_3NF.size() > 0){ // 3NF Decomposition
									Statement stnf31 = null;
									stnf31 = con.createStatement();
									String printStr = "";
									for (int i= 0; i<ck_FD_3NF.size();i++){
										printStr = printStr + ck_FD_3NF.get(i) + "->" + nck_FD_3NF.get(i) + " ";
									}
									
									String updatequr = "UPDATE " + result + "  SET WhyNotInNormalForm = '"
											+ printStr
											+ "', isInNormalForm = 'N' WHERE NAME='" + tblName
											+ "' AND NORMALFORM='3NF' ";
									stnf31.executeUpdate(updatequr);
									sqlDumpString.append(updatequr);
									sqlDumpString.append("\n");	
									decompDetails.append("3NF Decomposition\n");
		
									System.out.println("3NF Decomposition");
									
									List<String> Up = new ArrayList<String>();
									List<String> Down = new ArrayList<String>();
									int count = 0;
									String Relation;
									for (int i = 0; i<ck_FD_3NF.size();i++){
										if (Down.contains(ck_FD_3NF.get(i)) | Down.contains(nck_FD_3NF.get(i))) {
										    //ignore
										}else{
											if (Up.contains(ck_FD_3NF.get(i)) | Up.contains(nck_FD_3NF.get(i))) {
												count++;
												if (Up.contains(ck_FD_3NF.get(i))){
													Down.add(nck_FD_3NF.get(i));
													Relation = "R_"+ count + "(" + ck_FD_3NF.get(i)+"," + nck_FD_3NF.get(i) +")";
													System.out.println(Relation);
													decompDetails.append(Relation);
													decompDetails.append("\n");
												}else{
													Down.add(ck_FD_3NF.get(i));
													Relation = "R_"+ count + "(" + ck_FD_3NF.get(i)+"," + nck_FD_3NF.get(i) +")";
													System.out.println(Relation);
													decompDetails.append(Relation);
													decompDetails.append("\n");
						
												}
											}else{
												count++;
												Up.add(ck_FD_3NF.get(i));
												Down.add(nck_FD_3NF.get(i));
												Relation = "R_"+ count + "(" + ck_FD_3NF.get(i)+"," + nck_FD_3NF.get(i) +")";
												System.out.println(Relation);
												decompDetails.append(Relation);
												decompDetails.append("\n");
											}
										}
									}
									count++;
									String pr3 = "R_" + count + "(";
									for (int i=0;i<keysList.size();i++){
										pr3 = pr3 + keysList.get(i)+",";
									}
									for (int i = 0; i<Up.size();i++){
										if (i == Up.size()-1){
											pr3 = pr3 + Up.get(i) +")";
										}else{
											pr3 = pr3 + Up.get(i) +",";
										}
										
									}
									System.out.println(pr3);
									decompDetails.append(pr3);
									decompDetails.append("\n");
								}
								rs1 = st1.executeQuery("SELECT isInNormalForm FROM  " + result + "  WHERE NAME = '"
										+ tblName + "' AND NORMALFORM = '3NF'");
								sqlDumpString.append("SELECT isInNormalForm FROM  " + result + "  WHERE NAME = '"
										+ tblName + "' AND NORMALFORM = '3NF'");
								sqlDumpString.append("\n");	

								while (rs1.next()) {
									NForm1 = rs1.getString("isInNormalForm");
								}

								if (NForm1.equals("")) {
									st1.execute("UPDATE  " + result + "  SET isInNormalForm = 'Y' WHERE NAME = '"
											+ tblName + "' AND NORMALFORM = '3NF'");
									sqlDumpString.append("UPDATE  " + result + "  SET isInNormalForm = 'Y' WHERE NAME = '"
											+ tblName + "' AND NORMALFORM = '3NF'");
									sqlDumpString.append("\n");	
								}
							}
						}
						st1.close();
					} // end of for loop
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void tablesInilialize() {
	}
	
	
	// Final Output table generator
	public void resultTableGenerator()
	{
		Connection con = getConnection();
		Statement st = null;
		StringBuffer content;
		try {
			st = con.createStatement();
			ResultSet rsout=st.executeQuery("SELECT * FROM "+result);
			while(rsout.next())
			{
				content = new StringBuffer();
				content.append(rsout.getString(1));
				content.append(" \t ");
				content.append(rsout.getString(2));
				content.append(" \t ");
				content.append(rsout.getString(3));
				content.append(" \t ");
				content.append(rsout.getString(4));
				writeContent(content.toString());
			}
			st.close();
			writeContent(decompDetails.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	// write contents to a file 
	public void writeContent(String content){
		try {
			int f=0;
			File file = new File(outputFile);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
				f=1;
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(fw);
			if(f==1){
				bw.write("----------------------------");
				bw.write("\n");
				bw.write("TABLE \t FORM \t isInNormalForm \t WhyNotInNormalForm");
				bw.write("\n");
				bw.write("----------------------------");
				bw.write("\n");
				bw.write(content);
				bw.write("\n");
				bw.close();
			}
			else
			{
			bw.write(content);
			bw.write("\n");
			bw.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// Sql dump
		public void resultSqlDump() {
			try {
				int f = 0;
				File file = new File(SQLFile);
				// if file doesnt exists, then create it
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(sqlDumpString.toString());
				bw.write("\n");
				bw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
}