package eu.xlime.kafka.rdf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;

public class SparqlQueryFactory {

	private static final Logger log = LoggerFactory.getLogger(SparqlQueryFactory.class);
	
	public String microPostsByKeywordFilter(List<String> allowedKeywordFilters) {
		String qPattern = load("sparql/microPostsByKeywordFilter.rq");
		String result = qPattern.replaceAll("#ReplaceByFilter", filterOneOf("?keywordFilter", asStringLits(allowedKeywordFilters)));
		return result;
	}

	private List<String> asStringLits(List<String> values) {
		List<String> result = new ArrayList<String>();
		for (String val: values) {
			result.add(asStringLit(val));
		}
		return result;
	}

	private String asStringLit(String val) {
		StringBuilder sb = new StringBuilder();
		String quote = "\"";
		if (!val.startsWith(quote)) sb.append(quote);
		sb.append(val);
		if (!val.endsWith(quote)) sb.append(quote);
		return sb.toString();
	}

	private String filterOneOf(String var, Collection<String> values) {
		if (values.isEmpty()) return ""; //empty filter

		String s = orEq(var, values);
		return String.format("FILTER( %s )", s);
	}
	
	/**
	 * Returns a String with the format
	 * <code>
	 *   (var = values(1)) || (var = values(2)) || ...
	 * </code>
	 * 
	 * If <code>values</code> is empty, an empty string is returned.
	 * If <code>values</code> only has one value, only
	 * <pre>
	 *   var '=' values(0)
	 * </pre>
	 * is returned.
	 * 
	 * @param var
	 * @param values
	 * @return
	 */
	private String orEq(String var, Collection<String> values) {
		StringBuilder sb = new StringBuilder();
		if (values.size() == 1) {
			sb.append(String.format("%s = %s", var, values.iterator().next()));
		} else for (String val: values) {
			if (sb.length() > 0) sb.append(" || ");
			sb.append(String.format("(%s = %s)", var, val));
		}
		return sb.toString();
	}

	/**
	 * Loads a Sparql query (or query pattern) from disk or classpath.
	 * 
	 * @param path
	 * @return
	 */
	private String load(String path) {
		try {
			return loadFromFile(path);
		} catch (Exception e) {
			log.debug("Could not retrieve query(pattern) from file" + e.getLocalizedMessage() + ". Trying from classpath."); 
			try {
				return loadFromClassPath("/"+path);
			} catch (IOException e1) {
				throw new RuntimeException("Query resource not found", e1);
			}
		}	}

	private String loadFromClassPath(String path) throws IOException {
		InputStream inputStream = getClass().getResourceAsStream(path);
		return CharStreams.toString(new InputStreamReader(
			      inputStream, Charsets.UTF_8));
	}

	private String loadFromFile(String path) throws IOException {
		File f = new File(path);
		if (!f.exists()) throw new FileNotFoundException("File does not exist " + f.getAbsolutePath());
		return Files.toString(f, Charsets.UTF_8);
	}
}
