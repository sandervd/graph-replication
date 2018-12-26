package eu.europa.ec.contentlayer.pod.pojo;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

import java.util.HashMap;
import java.util.Map;

public class ModelBySubject extends AbstractRDFHandler {
    private Map<String, LinkedHashModel> models = new HashMap<>();
    private Map<String, String> bnodeInverseIndex = new HashMap<>();

    @Override
    public void handleStatement(Statement st) {
        String sub = st.getSubject().stringValue();
        // Triple is a statement about a bnode.
        if (st.getSubject() instanceof BNode) {
            // BNode is know, so we can add the statement to the applicable model.
            if (bnodeInverseIndex.containsKey(sub)) {
                String statementSubj = bnodeInverseIndex.get(sub);
                addStatementToModel(statementSubj, st);
            }
            else {
                // We encountered a bnode that hasn't been referenced in the object position.
                // We could handle this case, but don't know if needed.
                throw new RuntimeException("BNode unknown.");
            }
        }
        else {
            if (st.getObject() instanceof BNode) {
                bnodeInverseIndex.put(st.getObject().stringValue(), sub);
            }
            addStatementToModel(sub, st);
        }
    }

    public Map<String, LinkedHashModel> getModels() {
        return models;
    }

    private void addStatementToModel(String subject, Statement statement) {
        LinkedHashModel model = models.getOrDefault(subject, new LinkedHashModel());
        model.add(statement);
        models.put(subject, model);
    }
}
