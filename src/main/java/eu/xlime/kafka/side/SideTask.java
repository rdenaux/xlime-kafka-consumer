package eu.xlime.kafka.side;

import java.util.Properties;
import java.util.TimerTask;

import eu.xlime.kafka.msgproc.DatasetProcessor;

/**
 * Base class for all side tasks, which are timed tasks that can be executed parallell to 
 * {@link DatasetProcessor}s.
 * 
 * @author rdenaux
 *
 */
public abstract class SideTask extends TimerTask {

	protected SideTask(Properties props) {
		
	}

}
