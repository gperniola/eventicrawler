package crawlerTaccodiBacco;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Date;
import java.io.InputStream;

import org.json.*;

import utility.Converter;
import utility.Core;

public class Updater {

    public static String connURL = "jdbc:postgresql://127.0.0.1/PugliaEventi2?characterEncoding=utf8";
    //public static String connURL = "jdbc:postgresql://127.0.0.1/PugliaEventi?characterEncoding=utf8";
	//public static String connURL = "jdbc:postgresql://postgres/PugliaEventi?characterEncoding=utf8";

	public static String DB_User = "postgres";
	public static String DB_Password = "postgres";
	public static Boolean activateLinkExtractor = true;
	public static Boolean activateEventExtractor = true;
	public static Boolean activatew2v = false;
	public static Boolean activateWeatherExtractor = true;
	public static Boolean activatePrevisioniExtractor = true;
	public static Boolean activateConverter = false;

	public static void main (String args[]) throws Exception {
		Connection connDb = null;

		String resourceName = "/parametri.json";
		InputStream is = Updater.class.getResourceAsStream(resourceName);
		if (is == null) {
			throw new NullPointerException("Cannot find resource file " + resourceName);
		}

		JSONTokener tokener = new JSONTokener(is);
		JSONObject object = new JSONObject(tokener);

		connURL = "jdbc:postgresql://127.0.0.1/"+object.getString("database")+"?characterEncoding=utf8";
		DB_User = object.getString("user");
		DB_Password = object.getString("password");
		activateLinkExtractor = object.getBoolean("link");
		activateEventExtractor = object.getBoolean("event");
		activatew2v = object.getBoolean("w2v");
		activateWeatherExtractor = object.getBoolean("weather");
		activatePrevisioniExtractor = object.getBoolean("previsioni");
		activateConverter = object.getBoolean("converter");

		System.out.println("------------------------------------------------------------------------------[ PARAMETRI ]");
		System.out.println("DATABASE: " + object.getString("database") + " | " + "USER: " + DB_User + " | " + "PASSWORD: " + DB_Password);
		System.out.println("CONN URL: " + connURL);
		System.out.println("\n [1] LinkExtractor: " + activateLinkExtractor + "\n [2] EventExtractor: " + activateEventExtractor + "\n [3] w2v: " + activatew2v);
		System.out.println(" [4] WeatherExtractor: " + activateWeatherExtractor + "\n [5] PrevisioniExtractor: " + activatePrevisioniExtractor  + "\n [6] Converter: " + activateConverter);


		try
	    {
	    	Class.forName("org.postgresql.Driver");
			if (connDb == null) {
				connDb = DriverManager.getConnection(Updater.connURL, Updater.DB_User, Updater.DB_Password);
				//connDb = DriverManager.getConnection(Updater.connURL, "perniola", "perniola12319");
			}  
	    }
	    catch(ClassNotFoundException cnfe)
	    {
	        System.out.println("Error loading class!");
	        cnfe.printStackTrace();
	    }
		String query = "SELECT * from control";
		Statement st0 = connDb.createStatement();
		ResultSet rs0 = st0.executeQuery(query);
		rs0.next();
		Date last_up = rs0.getDate("last_update");

		//DEBUG_CODE
		System.out.println("\nULTIMO AGGIORNAMENTO DATABASE: " + last_up.toString() + "\n");

		System.out.println("------------------------------------------------------------------------------[ LinksExtractor ]");
		if(activateLinkExtractor)
			LinkExtractor.updateLinks(last_up);
		else System.out.println("Skipping LinkExtractor ...");

		System.out.println("------------------------------------------------------------------------------[ EventExtractor ]");
		if(activateEventExtractor)
			EventExtractor.eventExtract();
		else System.out.println("Skipping EventExtractor ...");

		System.out.println("------------------------------------------------------------------------------[ W2V ]");
		if(activatew2v) {
			//DEBUG_CODE
			System.out.println("Processing w2v ...");
			Core.eventsTow2v();
		}
		else System.out.println("Skipping w2v processing ...");

		System.out.println("------------------------------------------------------------------------------[ MeteoExtractor ]");
		if(activateWeatherExtractor)
			MeteoExtractor.extractPastMeteoData();
		else System.out.println("Skipping MeteoExtractor ...");

		System.out.println("------------------------------------------------------------------------------[ PrevisioniExtractor ]");
		if(activatePrevisioniExtractor)
			PrevisioniExtractor.extract7days();
		else System.out.println("Skipping PrevisioniExtractor ...");

		System.out.println("------------------------------------------------------------------------------[ Converter ]");
        if(activateConverter) {
			System.out.println("Converter.eventiTovec() ...");
			Converter.eventiTovec();
		}
		else System.out.println("Skipping w2v converter ...");


		//Save new update date to DB
		String up = "UPDATE control set last_update = ?";
		LocalDateTime ldt = LocalDateTime.now();
		PreparedStatement pt = connDb.prepareStatement(up);
		pt.setDate(1, java.sql.Date.valueOf(ldt.toLocalDate()));
		pt.execute();
		connDb.close();

        //DEBUG_CODE
		System.out.println("------------------------------------------------------------------------------[ Updater ]");
        System.out.println("DATABASE SALVATO E CHIUSO.");
		
	}
}
