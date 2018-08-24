package eu.europa.ec.contentlayer.pod.pojo;

import java.io.Serializable;

public class RdfTransaction implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tripleSubject;

	private Boolean delete = Boolean.FALSE;
	
	private String id;
	private String name;

	public String getTripleSubject() {
		return tripleSubject;
	}

	public void setTripleSubject(String tripleSubject) {
		this.tripleSubject = tripleSubject;
	}

	public void setDelete() {
		this.delete = Boolean.TRUE;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
