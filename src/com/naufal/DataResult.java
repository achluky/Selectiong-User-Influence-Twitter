package com.naufal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.mysql.jdbc.Connection;

public class DataResult {
	public static Connection connect = null;
    @SuppressWarnings("rawtypes")
	public List<Map> confMysql;
    public String user;
    public String password;
    public String database;

	public DataResult() throws ClassNotFoundException, SQLException{
        System.out.println("Load configuration connection .....");
        String user = "root";
        String password = "";
        String database = "lexicon";
        System.out.println("Connecting.....");
        Class.forName("com.mysql.jdbc.Driver");
        connect = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/"+database, user, password);
    }
    public static ResultSet sql(String sqlString) throws SQLException{
        Statement statement = connect.createStatement();
        return  statement.executeQuery(sqlString);
    }
    public void sqlDelete(String table, String whereField, String whereVal) throws SQLException{
        PreparedStatement preparedStatement = connect.prepareStatement("delete from "+table+" where "+whereField+"= ? ; ");
        preparedStatement.setString(1, whereVal);
        preparedStatement.executeUpdate();
    }
    public void sqlInsert(String sqlInsert) throws SQLException{
        PreparedStatement preparedStatement = connect.prepareStatement(sqlInsert);
        preparedStatement.executeUpdate();
    }
    public void sqlUpdate(String sql) throws SQLException{
		Statement statement = connect.createStatement();
		System.out.println(sql);
		statement.execute(sql);
    }
    public void closex() throws SQLException{
        connect.close();
    }
    
    /**
     * Main function Test
     * @param args
     * @throws SAXException
     * @throws IOException
     * @throws ParserConfigurationException
     * @throws SQLException 
     * @throws ClassNotFoundException 
     */
     @SuppressWarnings("resource")
	 public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException, SQLException, ClassNotFoundException{
    	 DataResult  conect = new DataResult();
    	 String sCurrentLine;
		 BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\DIDSI-IPB\\workspace\\Selecting-KeyFacebook\\data-in\\comunication_exsternal\\data.txt"));
		 while ((sCurrentLine = br.readLine()) != null) {
				String str[] =  sCurrentLine.split(";");
		        String sql = "Insert Into data_CE (`akun`, `follower`) values ('"+str[0]+"', "+str[1]+")";
		        conect.sqlInsert(sql);
		        System.out.println(sql);
		 }
     }
}
