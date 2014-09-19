package aifb.kit.xlime.kafkaextracor;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;


public class CallTextAnnotator {
	public static String getAnnotedData(String model, String Source, String lang1, String lang2, String kb, String url){
	
		String urlParameters="model="+model+"&source="+Source+"&lang1="+lang1+"&kb="+kb+"&lang2="+lang2;		
		HttpURLConnection connection = null;
		StringBuffer response = new StringBuffer();
		try
		{
		URL url1 = new URL(url);
		connection = (HttpURLConnection)url1.openConnection();
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setInstanceFollowRedirects(false); 
		connection.setRequestMethod("POST"); 
		connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
		connection.setRequestProperty("charset", "utf-8");
		connection.setRequestProperty("Content-Length", "" + Integer.toString(urlParameters.getBytes().length));
		connection.setUseCaches (false);
		DataOutputStream wr = new DataOutputStream(connection.getOutputStream ());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();
		
		String line;
		BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		
		while ((line = reader.readLine()) != null) {
			response.append(line+"\n");
		}
		reader.close();
		}
		catch (MalformedURLException e) {
	          e.printStackTrace();
	      } catch (ProtocolException e) {
	          e.printStackTrace();
	      } catch (IOException e) {
	          e.printStackTrace();
	      }
		finally
	      {
	          //close the connection, set all objects to null
	          connection.disconnect();
	          connection = null;
	      }
		return response.toString();
	
	}
	public static void main(String[] args)
	{
		String model= "ngram";
		String Source = "Listening To Floyd Mayweather Jr. Struggle To Read A Brief Radio Drop Is Painful Reading is fundamental.";
		String lang1="en";
		String lang2="en";
		String kb = "dbpedia";
		String url=" http://km.aifb.kit.edu/services/text-annotation-all/?";
		System.out.println(getAnnotedData(model,Source,lang1,lang2,kb,url));
		
	}
}