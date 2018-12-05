package crawlerTaccodiBacco;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class PrevisioniExtractor {
	public static boolean error = false;

	public static void extract7days() throws Exception {
		Connection connDb = null;

		try {
			Class.forName("org.postgresql.Driver");
			if (connDb == null) {
				connDb = DriverManager.getConnection("jdbc:postgresql://127.0.0.1/PugliaEventi?characterEncoding=utf8",
						"postgres", "postgres");
			}
		} catch (ClassNotFoundException cnfe) {
			System.out.println("Error loading class!");
			cnfe.printStackTrace();
		}
		LocalDateTime date = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
		String query = "SELECT * from eventi WHERE data_a >= '" + formatter.format(date)
				+ "' AND  data_da <= '" + formatter.format(date) + "' ";
		//System.out.println(query);
		Statement st0 = connDb.createStatement();
		ResultSet rs0 = st0.executeQuery(query);


		//DEBUG_CODE -------------------------
		String queryCount = "SELECT COUNT(*) from eventi where data_a >= '" + formatter.format(date)
				+ "' AND  data_da <= '" + formatter.format(date) + "' ";
		Statement stCount = connDb.createStatement();
		ResultSet rsCount = stCount.executeQuery(queryCount);
		rsCount.next();
		System.out.println("PrevisioniExtractor.java: extracting weather predictions for " + rsCount.getString(1) +" events ...");
		int callsCount = 0;
		int eventsProcessed = 0;
		int errorsFound = 0;
		int istatErrorsFound = 0;
		//-----------------------------------------


		String updateOld = "DELETE FROM previsioni_eventi WHERE true";
		Statement st = connDb.createStatement();
		st.execute(updateOld);
		updateOld = "DELETE FROM previsioni_comuni WHERE true";
		st = connDb.createStatement();
		st.execute(updateOld);
		

		// Check meteo passato

		while (rs0.next()) {
			error = false;
			
			String link = rs0.getString("link");
			Date da = rs0.getDate("data_da");
			Date a = rs0.getDate("data_a");
			Calendar start = Calendar.getInstance();
			Calendar start2 = Calendar.getInstance();
			Calendar end = Calendar.getInstance();
			end.setTime(a);
			String comune = rs0.getString("comune");

			//METEO_CODE
			String idComune = getIstatDb(comune,connDb);
			if (idComune == null){istatErrorsFound++;}
			//--------------

			ArrayList<Date> dateev = new ArrayList();
			String linkMeteo = "https://www.tempoitalia.it/meteo/" + comune.replace(" ", "-").replace("'", "-");
			
			if (DateUtils.isSameDay(start, end)) {
				//METEO_CODE controllo se ho già ho una previsione per la città/data dell'evento
				int idPrevisioni = checkPrevisioniDb(idComune, new java.sql.Date(a.getTime()), connDb);
				if(idPrevisioni == -1 && idComune != null) dateev.add(a); //se non ho una previsione, inserisci la data in array
				if (idPrevisioni != -1) AddPrevisioneEvento(link,rs0.getString("titolo"),new java.sql.Date(a.getTime()),rs0.getInt("autoid"),idPrevisioni,connDb);
				//---------------------------------------
			} else {
				Calendar start7p = start;
				start7p.add(Calendar.DATE,7);
				//Se fine evento e oggi sono diversi (non è singola data nè ultimo giorno) è nel centro. Previsioni per max 7 giorni, quindi si sceglie il min fra oggi+7 e fine evento
				Date endD = (Date) minDate(start7p.getTime(),end.getTime());
				Calendar d = Calendar.getInstance();
				d.setTime(endD);
				for (Date dat = start2.getTime(); start2.before(d); start2.add(Calendar.DATE,1), dat = start2.getTime()) {
					//METEO_CODE controllo se ho già ho una previsione per la città/data dell'evento
					int idPrevisioni = checkPrevisioniDb(idComune, new java.sql.Date(dat.getTime()), connDb);
					if(idPrevisioni == -1 && idComune != null){
						dateev.add(dat); //se non ho una previsione, inserisci la data in array
					}
					if (idPrevisioni != -1){
						AddPrevisioneEvento(link,rs0.getString("titolo"),new java.sql.Date(dat.getTime()),rs0.getInt("autoid"),idPrevisioni,connDb);
					}
					//--------------------------------------------
				}
			}

			if(!dateev.isEmpty()) {
				getMeteoData(linkMeteo, idComune, comune, dateev, connDb);
				callsCount++;
				for(int i = 0; i < dateev.size(); i++) {
					Date dat = dateev.get(i);
					int idPrevisioni = checkPrevisioniDb(idComune, new java.sql.Date(dat.getTime()), connDb);
					if (idPrevisioni != -1){
						AddPrevisioneEvento(link,rs0.getString("titolo"), new java.sql.Date(dat.getTime()),rs0.getInt("autoid"),idPrevisioni,connDb);
					}
				}
				Thread.sleep(1000);
			}


			if(error!= true) {
				String up = "UPDATE eventi SET previsioni_bool = 1 where link = ?";
				PreparedStatement st3 = connDb.prepareStatement(up);
				st3.setString(1, link);
				st3.execute();
			} if(error== true) {
				String up = "UPDATE eventi SET previsioni_bool = -1 where link = ?";
				PreparedStatement st3 = connDb.prepareStatement(up);
				st3.setString(1, link);
				st3.execute();
				errorsFound++;
			}

			//DEBUG_CODE
			eventsProcessed++;
			if((eventsProcessed % 100) == 0) {
				System.out.println("Processed events: " + eventsProcessed + " ...");
				System.out.println("Number of weather calls made: " + callsCount + " ...");
			}
			//-----------------
			 
			//Thread.sleep(5000);
		}
		connDb.close();
		//DEBUG_CODE
		System.out.println("TOTAL number of events processed: " + eventsProcessed);
		System.out.println("TOTAL number of weather calls: " + callsCount);
		System.out.println("TOTAL number of weather errors detected: " + errorsFound);
		System.out.println("TOTAL number of istat code errors detected: " + istatErrorsFound);
		//---------------------
	}

		public static Comparable minDate(Comparable c1, Comparable c2) {
			if (c1.compareTo(c2) < 0)
				return c1;
			else
				return c2;
		}

		
	public static void getMeteoData(String linkMeteo, String idComune, String comune, ArrayList<Date> dateev, Connection connDb) throws Exception  {
			 try {
				 URL url = new URL(linkMeteo);
					//System.out.println(linkMeteo);
				 URLConnection conn = url.openConnection();
				 conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36");
				 conn.setRequestProperty("Accept","text/html");
				 conn.setRequestProperty("Connection","keep-alive");
				 conn.setConnectTimeout(3000);
		
				 conn.connect();
				 InputStream stream = conn.getInputStream();
				 InputStreamReader reader = new InputStreamReader(stream);
				 BufferedReader in = new BufferedReader(reader);
				 String inputLine;
				 String html ="";
			        while ((inputLine = in.readLine()) != null) 
			        	html=html+" "+inputLine;
			        
			     Document doc = Jsoup.parse(html);   
			     
			     //Prendo comunque tutti e 7 i giorni
			     ArrayList<Integer> giorni = new ArrayList();
			     ArrayList<Double> temperature = new ArrayList();
			     ArrayList<Integer> venti = new ArrayList();
			     //tags
			     ArrayList<Integer> sereni = new ArrayList();
			     ArrayList<Integer> coperti = new ArrayList();
			     ArrayList<Integer> poco_nuvolosi = new ArrayList();
			     ArrayList<Integer> piogge = new ArrayList();
			     ArrayList<Integer> temporali = new ArrayList();
			     ArrayList<Integer> nebbie = new ArrayList();
			     ArrayList<Integer> nevi = new ArrayList();
			     
			     for(int i = 1 ; i < 8; i ++) {
			    	 //Giorni
			    	 int giorno = Integer.parseInt(doc.select("#tableHours > div.content > table > tbody > tr:nth-child("+i+") > td.timeweek > a").html().split(" ")[1]);
			    	 giorni.add(giorno);
			    	 
			    	 //Temperatura
			    	 double tempMax = Double.parseDouble(doc.select("#tableHours > div.content > table > tbody > tr:nth-child("+i+") > td.tempmax").html().replace("°C", ""));
			    	 double tempMin = Double.parseDouble(doc.select("#tableHours > div.content > table > tbody > tr:nth-child("+i+") > td.tempmin").html().replace("°C", ""));		 
			    	 double temMed = (tempMax+tempMin)/2;
			    	 temperature.add(temMed);
			    	 
			    	 //Vento
			    	 int vento = Integer.parseInt(doc.select("#tableHours > div.content > table > tbody > tr:nth-child("+i+") > td.wind > span.speed").html().replace(" Km/h", ""));
			    	 venti.add(vento);
			    	 
			    	 //Tags
			    	 String dec = doc.select("#tableHours > div.content > table > tbody > tr:nth-child("+i+") > td.skyDesc").html();
			    	 
			    	 if(dec.toLowerCase().contains("sereno")) {
			    		 sereni.add(1);
			    	 }else {sereni.add(0);}
			    	 
			    	 if(dec.toLowerCase().contains("coperto")) {
			    		 coperti.add(1);
			    	 }else {coperti.add(0);}
			    	 
			    	 if(dec.toLowerCase().contains("nuvoloso") || dec.toLowerCase().contains("nubi") ) {
			    		 poco_nuvolosi.add(1);
			    	 }else {poco_nuvolosi.add(0);}
			    	 
			    	 if(dec.toLowerCase().contains("pioggia") || dec.toLowerCase().contains("acquazzoni") || dec.toLowerCase().contains("piovaschi") || dec.toLowerCase().contains("pioviggine") || dec.toLowerCase().contains("rovesci")) {
			    		 piogge.add(1);
			    	 }else {piogge.add(0);}

			    	 if(dec.toLowerCase().contains("temporali")) {
			    		 temporali.add(1);
			    	 }else {temporali.add(0);}
			    	 
			    	 if(dec.toLowerCase().contains("neve")) {
			    		 nevi.add(1);
			    	 }else {nevi.add(0);}
			    	 
			    	 if(dec.toLowerCase().contains("nebbia")) {
			    		 nebbie.add(1);
			    	 }else {nebbie.add(0);}
			    	
			     }
			     
			     for(int i = 0; i < dateev.size(); i++) {
			     Date dat = dateev.get(i);
			     int giorno = dat.getDate();
			     int index = giorni.indexOf(giorno);

			     int primavera = 0;
			     int estate = 0;
			     int autunno = 0;
			     int inverno = 0;
				 
				 String seasons[] = {
						    "Winter", "Winter",
						    "Spring", "Spring", "Spring",
						    "Summer", "Summer", "Summer",
						    "Fall", "Fall", "Fall",
						    "Winter"
				};
				String stagione =  seasons[ dat.getMonth() ];
				
				if(stagione.equals("Winter")) {
					inverno = 1;
				}
				if(stagione.equals("Spring")) {
					primavera = 1;
				}
				if(stagione.equals("Summer")) {
					estate = 1;
				}
				if(stagione.equals("Fall")) {
					autunno = 1;
				}
			     
				//Inserisci nel db
		    	 String check = "SELECT * from previsioni_comuni where idcomune = ? AND data = ?";
		    	 PreparedStatement st1 = connDb.prepareStatement(check);
		    	 st1.setString(1, idComune);
		    	 st1.setDate(2, new java.sql.Date(dat.getTime()));
		    	 ResultSet rs = st1.executeQuery();
		    	
		    	 if(!rs.next()){
		    		 String q = "INSERT INTO previsioni_comuni(idcomune,comune,data,primavera,estate,autunno,inverno,sereno,coperto,poco_nuvoloso,pioggia,temporale,nebbia,neve,temperatura,velocita_vento) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		    		 PreparedStatement st2 = connDb.prepareStatement(q);
			    	 st2.setString(1, idComune);
			    	 st2.setString(2, comune);
			    	 st2.setDate(3, new java.sql.Date(dat.getTime()));
			    	 st2.setInt(4, primavera);
			    	 st2.setInt(5, estate);
			    	 st2.setInt(6, autunno);
			    	 st2.setInt(7, inverno);
			    	 st2.setInt(8, sereni.get(index));
			    	 st2.setInt(9, coperti.get(index));
			    	 st2.setInt(10, poco_nuvolosi.get(index));
			    	 st2.setInt(11, piogge.get(index));
			    	 st2.setInt(12, temporali.get(index));
			    	 st2.setInt(13, nebbie.get(index));
			    	 st2.setInt(14, nevi.get(index));
			    	 st2.setDouble(15, temperature.get(index));
			    	 st2.setInt(16, venti.get(index));
			    	 st2.execute();
		    	 }
				
			     }
			 }catch(Exception e) {error = true; e.printStackTrace();}
		}



		public static String getMonth(int month) {
		    return new DateFormatSymbols(Locale.ITALIAN).getMonths()[month];
		}



	//Controlla che il comune abbia un codice istat
	public static String getIstatDb(String comune, Connection connDb) throws NoSuchElementException,Exception {
		String istatComuneFound = null;

		try {

			//Controlla codice istat partendo da nome comune
			String checkIstat = "SELECT istat from comuni where comune = ?";
			PreparedStatement statement = connDb.prepareStatement(checkIstat);
			statement.setString(1, comune);
			ResultSet rs = statement.executeQuery();

			if (!rs.next()) {
				throw new NoSuchElementException();
			} else {
				istatComuneFound = rs.getString("istat");
			}
		}catch(NoSuchElementException e){error = true; System.out.println("NoSuchElementException: Istat code of " + comune + " not found");}
		catch(Exception e) {error = true; e.printStackTrace();}

		return istatComuneFound;
	}



	//Controlla se il meteo per la città e data selezionata sia già stato estratto
	//se non è stato ancora estratto, restituisce -1, altrimenti restituisce l'id del meteo trovato.
	public static int checkPrevisioniDb(String idComune, Date data, Connection connDb) throws Exception{
		int idPrevisioniFound = -1;
		try {
			//Controlla se è presente gia' il meteo per comune e data selezionata
			String checkMeteo = "SELECT autoid from previsioni_comuni where idcomune = ? AND data = ?";
			PreparedStatement statement2 = connDb.prepareStatement(checkMeteo);
			statement2.setString(1, idComune);
			statement2.setDate(2, new java.sql.Date(data.getTime()));
			ResultSet rs2 = statement2.executeQuery();

			if(rs2.next()){idPrevisioniFound = rs2.getInt("autoid");}

		}catch(Exception e) {error = true; e.printStackTrace();}
		return idPrevisioniFound;
	}


	//Aggiunge l'id del meteo per l'evento selezionato nella tabella meteo_eventi
	public static void AddPrevisioneEvento(String link, String titolo,  Date data, int idEvento, int idPrevisione, Connection connDb) throws Exception{
		try{
			//Inserisci il meteo per l'evento nella tabella meteo_eventi
			String check = "SELECT * from previsioni_eventi where link =? AND titolo = ? AND dataevento = ?";
			PreparedStatement st1 = connDb.prepareStatement(check);
			st1.setString(1, link);
			st1.setString(2, titolo);
			st1.setDate(3, new java.sql.Date(data.getTime()));
			ResultSet rs = st1.executeQuery();

			if(!rs.next()) {
				String insert = "INSERT INTO previsioni_eventi(link,titolo,dataevento,idevento,idprevisione) VALUES (?,?,?,?,?)";
				PreparedStatement st2 = connDb.prepareStatement(insert);
				st2.setString(1, link);
				st2.setString(2, titolo);
				st2.setDate(3, new java.sql.Date(data.getTime()));
				st2.setInt(4, idEvento);
				st2.setInt(5, idPrevisione);
				st2.execute();
			}
		}catch(Exception e) {error = true; e.printStackTrace();}
	}
		
		

}
