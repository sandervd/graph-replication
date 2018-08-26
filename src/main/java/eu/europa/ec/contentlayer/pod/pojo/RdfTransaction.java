package eu.europa.ec.contentlayer.pod.pojo;

import java.io.Serializable;

public class RdfTransaction implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Boolean delete = Boolean.FALSE;

	private String Statements;
	
	public void setDelete() {
		this.delete = Boolean.TRUE;
	}

	public String getStatements() {
		return Statements;
	}

	public void setStatements(String statements) {
		Statements = statements;
	}

}
