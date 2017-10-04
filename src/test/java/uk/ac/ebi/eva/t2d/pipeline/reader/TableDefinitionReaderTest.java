package uk.ac.ebi.eva.t2d.pipeline.reader;

import org.junit.Test;
import uk.ac.ebi.eva.t2d.jobs.readers.TableDefinitionReader;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class TableDefinitionReaderTest {

    private static final String TABLE_DEFINITION_FILE = "/t2d/table-definition.tsv";
    private static final String TABLE_NAME = "test-table";

    @Test
    public void readTableDefinition() throws Exception {
        TableDefinitionReader reader = new TableDefinitionReader(getResource(TABLE_DEFINITION_FILE), TABLE_NAME);
        T2DTableStructure tableStructure = reader.read();
        assertNotNull(tableStructure);
        assertEquals(5, tableStructure.getFields().size());
        tableStructure.getFields().stream().forEach(entry -> {
            switch (entry.getKey()) {
                case "col-string":
                    assertEquals(String.class, entry.getValue());
                    break;
                case "col-boolean":
                    assertEquals(Boolean.class, entry.getValue());
                    break;
                case "col-integer":
                    assertEquals(Integer.class, entry.getValue());
                    break;
                case "col-float":
                    assertEquals(Float.class, entry.getValue());
                    break;
                case "col-double":
                    assertEquals(Double.class, entry.getValue());
                    break;
                default:
                    assertFalse("Unexpected class type found '" + entry.getValue() + "'", true);
            }
        });
        assertEquals(TABLE_NAME, tableStructure.getTableName());
    }

}
