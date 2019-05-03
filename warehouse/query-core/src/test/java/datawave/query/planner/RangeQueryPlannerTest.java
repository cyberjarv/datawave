package datawave.query.planner;

import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GTE_OP;
import static datawave.query.testframework.RawDataManager.LTE_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

/**
 * Tests for different types of string and numeric range specifications.
 */
public class RangeQueryPlannerTest extends AbstractFunctionalQuery {

    private static final Logger log = Logger.getLogger(RangeQueryPlannerTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.NUM.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));

        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }

    public RangeQueryPlannerTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testSingleValue() throws Exception {
        log.info("------  testSingleValue  ------");
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "'";
            String expected = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "')";
            String plan = getPlan(query, true, true);
            Assert.assertEquals(expected, plan);
        }
    }
    
    @Test
    public void testRangeWithTerm() throws Exception {
        for (final TestCities city : TestCities.values()) {
            String query = "((" + CityField.NUM.name() + LTE_OP + "100)" + AND_OP + "(" + CityField.NUM.name() + GTE_OP + "100))" + AND_OP
                            + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            String expected = "((" + CityField.NUM.name() + LTE_OP + "100)" + AND_OP + "(" + CityField.NUM.name() + GTE_OP + "100))" + AND_OP
                    + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            String plan = getPlan(query, true, true);
            Assert.assertEquals(expected, plan);
        }
    }
    
    @Test
    public void testSingleValueAndMultiFieldWithParens() throws Exception {
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + AND_OP + "(" + CityField.NUM.name() + LTE_OP + "20" + AND_OP + CityField.NUM.name() + GTE_OP + "20)";
            String plan = getPlan(query, true, true);
            Assert.assertEquals(query, plan);
        }
    }
    
    @Test
    public void testSingleValueAndMultiFieldNoParens() throws Exception {
        log.info("------  testSingleValueAndMultiFieldNoParens  ------");
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "'"
                            + AND_OP + CityField.NUM.name() + LTE_OP + "20" + AND_OP + CityField.NUM.name() + GTE_OP + "20";
            String plan = getPlan(query, true, true);
            Assert.assertEquals(query, plan);
        }
    }
    
    @Test
    public void testSingleValueOrMultiFieldWithParens() throws Exception {
        log.info("------  testSingleValueOrMultiFieldWithParens  ------");
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + OR_OP + "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.NUM.name() + GTE_OP + "100)";
            String plan = getPlan(query, true, true);
            Assert.assertEquals(query, plan);
        }
    }
    
    @Test
    public void testMultiFieldsNoResults() throws Exception {
        log.info("------  testMultiFieldsNoResults  ------");
        String state = "'ohio'";
        String qState = "(" + CityField.STATE.name() + LTE_OP + state + AND_OP + CityField.STATE.name() + GTE_OP + state + ")";
        
        String cont = "'europe'";
        String qCont = "(" + CityField.CONTINENT.name() + LTE_OP + cont + AND_OP + CityField.CONTINENT.name() + GTE_OP + cont + ")";
        String qNum = "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.NUM.name() + GTE_OP + "100)";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + AND_OP + qState + AND_OP + qCont + AND_OP + qNum;
            String plan = getPlan(query, true, true);
            Assert.assertEquals(query, plan);
        }
    }
    
    @Test
    public void testRangeOpsInDiffSubTree() throws Exception {
        log.info("------  testRangeOpsInDiffSubTree  ------");
        String city = TestCities.rome.name();
        String query = "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.CITY.name() + EQ_OP + "'" + city + "')" + AND_OP + CityField.NUM.name()
                        + GTE_OP + "100";
        String plan = getPlan(query, true, true);
        Assert.assertEquals(query, plan);
    }
    
    @Test
    public void testRangeInOut() throws Exception {
        log.info("------  testRangeInOut  ------");
        String city = TestCities.rome.name();
        String query = "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.NUM.name() + GTE_OP + "100)";
        this.logic.setMaxValueExpansionThreshold(2);
        String plan = getPlan(query, true, true);
        Assert.assertEquals(query, plan);
    }
    
    @Test
    public void testRangeOrExp() throws Exception {
        log.info("------  testRangeOrExp  ------");
        String start = "'e'";
        String end = "'r'";
        String[] stateMatches = new String[] {"missouri", "lle-de-france", "michigan", "london", "ohio"};
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + CityField.CITY.name() + EQ_OP + "'" + city.name()
                            + "-extra')" + AND_OP + "(" + CityField.STATE.name() + GTE_OP + start + AND_OP + CityField.STATE.name() + LTE_OP + end + ")";
            String[] cityMatches = new String[] {city.name()};
            if (city.name().equals("london")) {
                cityMatches = new String[] {city.name(), "london-extra"};
            }
            StringBuilder expected = new StringBuilder();
//            String expected = "((((ASTEvaluationOnly = true) && (" + CityField.CITY.name() + ") == '" + city.name() + "' && (" + CityField.STATE.name() + " >= 'e' && \" + CityField.STATE.name() + \" <= 'r'))) && " +
//                    "(CITY_STATE == 'london\uDBFF\uDFFFmissouri' || " +
//                    "CITY_STATE == 'london\uDBFF\uDFFFlle-de-france' || " +
//                    "CITY_STATE == 'london\uDBFF\uDFFFmichigan' || " +
//                    "CITY_STATE == 'london\uDBFF\uDFFFlondon' || " +
//                    "CITY_STATE == 'london\uDBFF\uDFFFohio')) || " +
//                    "(((ASTEvaluationOnly = true) && (CITY == 'london-extra' && (STATE >= 'e' && STATE <= 'r'))) && " +
//                    "CITY_STATE >= 'london-extra\uDBFF\uDFFFe' && " +
//                    "CITY_STATE <= 'london-extra\uDBFF\uDFFFr'))";
            String plan = getPlan(query, true, true);
            Assert.assertEquals(expected, plan);
        }
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorRangeOpsInDiffSubTree() throws Exception {
        log.info("------  testErrorRangeOpsInDiffSubTree  ------");
        String city = TestCities.rome.name();
        String query = CityField.NUM.name() + LTE_OP + "100" + AND_OP + "(" + CityField.CITY.name() + EQ_OP + "'" + city + "'" + OR_OP + CityField.NUM.name()
                        + GTE_OP + "100)";
        ((DefaultQueryPlanner) logic.getQueryPlanner()).setExecutableExpansion(false);
        String plan = getPlan(query, true, true);
    }
    
    @Test
    public void testAvoidErrorRangeOpsInDiffSubTreeWithExpansion() throws Exception {
        log.info("------  testErrorRangeOpsInDiffSubTree  ------");
        String city = TestCities.rome.name();
        String query = CityField.NUM.name() + LTE_OP + "100" + AND_OP + "(" + CityField.CITY.name() + EQ_OP + "'" + city + "'" + OR_OP + CityField.NUM.name()
                        + GTE_OP + "100)";
        String plan = getPlan(query, true, true);
        Assert.assertEquals(query, plan);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorRangeGTE() throws Exception {
        log.info("------  testErrorRangeGTE  ------");
        String query = "(" + CityField.NUM.name() + GTE_OP + "99" + AND_OP + CityField.NUM.name() + GTE_OP + "121)";
        String plan = getPlan(query, true, true);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
