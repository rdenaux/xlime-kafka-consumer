package eu.xlime.kafka.side;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class SideTaskFactory {

	static final Logger log = LoggerFactory.getLogger(SideTaskFactory.class);
	
	private final ClassLoader clsLoader;
	
	public SideTaskFactory(ClassLoader clsLoader) {
		super();
		this.clsLoader = clsLoader;
	}

	public Optional<SideTask> createInstance(
			String instClassifierFQN, Properties props) {
		Class<? extends SideTask> cls = loadClass(instClassifierFQN);
		SideTask result = createInstance(cls, props);
		return Optional.of(result);
	}

	public <T> T createInstance(
			Class<T> cls, Properties props) {
		T result;		
		try {
			Class[] consParam = new Class[]{Properties.class};
			Constructor<T> constructor = cls.getConstructor(consParam);
			Object[] consObj = new Object[]{props};
			log.trace("Creating instance (type: "+cls+")...");
			result = constructor.newInstance(consObj); 
			log.trace("Creating instance (type: "+cls+")...done.");
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("Error finding constructor of "
					+ "configured class " + cls, e);
		} catch (SecurityException e) {
			throw new RuntimeException("Error accessing constructor of "
					+ "configured class " + cls, e);
		} catch (InstantiationException e) {
			throw new RuntimeException("Error creating " + cls, e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Error creating " + cls, e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Error creating " + cls, e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException("Error creating " + cls, e);
		}
		return result;
	}

	private <T> Class<T> loadClass(String instClassifierFQN) {
		try {
			return (Class<T>) clsLoader.loadClass(instClassifierFQN);
		} catch (Exception e) {
			ClassLoader current = getClass().getClassLoader();
			if (current.equals(clsLoader))
				throw new RuntimeException("Class was not loaded: Please check "+instClassifierFQN + " is on application classpath. Loaded with " + clsLoader, e);
			else try {
				return (Class<T>) current.loadClass(instClassifierFQN);
			} catch (Exception e1) {
				throw new RuntimeException("Class was not loaded: Please check "+instClassifierFQN + " is on application classpath. Tried with " + clsLoader + " and " + current, e1);
			}
		}
	}

}
