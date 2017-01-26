package edu.uci.ics.textdb.dataflow.nlpextractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.relation.RelationManager;

/**
 * @author Feng [sam0227]
 */
public class NlpExtractorTest {
    
    public static final String ONE_SENTENCE_TABLE = "nlp_test_two_sentence";
    public static final String TWO_SENTENCE_TABLE = "nlp_test_one_sentence";
    
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        relationManager.createTable(ONE_SENTENCE_TABLE, "../index/test_tables/" + ONE_SENTENCE_TABLE, 
                NlpExtractorTestConstants.SCHEMA_ONE_SENTENCE, LuceneAnalyzerConstants.standardAnalyzerString());
        relationManager.createTable(TWO_SENTENCE_TABLE, "../index/test_tables/" + TWO_SENTENCE_TABLE, 
                NlpExtractorTestConstants.SCHEMA_TWO_SENTENCE, LuceneAnalyzerConstants.standardAnalyzerString());
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(ONE_SENTENCE_TABLE);
        relationManager.deleteTable(TWO_SENTENCE_TABLE);
    }
    
    // table is cleared after each test case
    @After
    public void deleteData() throws TextDBException {
        RelationManager.getRelationManager().deleteTuples(ONE_SENTENCE_TABLE, new MatchAllDocsQuery());
        RelationManager.getRelationManager().deleteTuples(TWO_SENTENCE_TABLE, new MatchAllDocsQuery());
    }
    
    /**
     * @param NLP_TEST_TABLE
     * @param attributes
     * @param nlpTokenType
     * @return
     * @throws Exception
     * @about Using NlpExtractor to get all returned results from
     *        NLP_TEST_TABLE, return as a list of tuples
     */
    public List<ITuple> getQueryResults(String tableName, List<String> attributeNames,
            NlpPredicate.NlpTokenType nlpTokenType) throws Exception {
        return getQueryResults(tableName, attributeNames, nlpTokenType, Integer.MAX_VALUE, 0);
    }
    
    public List<ITuple> getQueryResults(String tableName, List<String> attributeNames,
            NlpPredicate.NlpTokenType nlpTokenType, int limit, int offset) throws Exception {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(tableName);

        NlpPredicate nlpPredicate = new NlpPredicate(nlpTokenType, attributeNames);
        NlpExtractor nlpExtractor = new NlpExtractor(nlpPredicate);
        nlpExtractor.setInputOperator(scanSource);

        nlpExtractor.setLimit(limit);
        nlpExtractor.setOffset(offset);
        
        ITuple nextTuple = null;
        List<ITuple> results = new ArrayList<ITuple>();
        
        nlpExtractor.open();
        while ((nextTuple = nlpExtractor.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        nlpExtractor.close();
        
        return results;
    }

    /**
     * Scenario 1: Test getNextTuple with only one span in the return list Text
     * : Microsoft is a organization. Search for all NE_ALL token types
     *
     * @throws Exception
     */
    @Test
    public void getNextTupleTest1() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest1Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }

        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL);

        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest1ResultTuples();
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario 2: Test getNextTuple with more than one span in the return list
     * Text: Microsoft, Google and Facebook are organizations Search for all
     * NE_ALL token types
     */
    @Test
    public void getNextTupleTest2() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest2Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }
        
        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest2ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);

    }

    /**
     * Scenario 3: Test getNextTuple with more than one span in the return list
     * and with different recognized classes. Text: Microsoft, Google and
     * Facebook are organizations and Donald Trump and Barack Obama are persons.
     * Search for all NE_ALL token types
     */
    @Test
    public void getNextTupleTest3() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest3Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }
        
        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest3ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 4:Test getNextTuple with more than one span in the return list
     * and with different recognized classes and more than one fields in the
     * source tuple.
     * <p>
     * Sentence1: Microsoft, Google and Facebook are organizations. Sentence2:
     * Donald Trump and Barack Obama are persons. Search for all NE_ALL token
     * types
     */
    @Test
    public void getNextTupleTest4() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest4Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(TWO_SENTENCE_TABLE, tuple);
        }
        
        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        String attribute2 = NlpExtractorTestConstants.SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<ITuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest4ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 5:Test getNextTuple using two fields:
     * <p>
     * Sentence1: Microsoft, Google and Facebook are organizations. Sentence2:
     * Donald Trump and Barack Obama are persons.
     * <p>
     * Only search the second field for all NE_ALL token types
     */
    @Test
    public void getNextTupleTest5() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest4Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(TWO_SENTENCE_TABLE, tuple);
        }
        
        String attribute = NlpExtractorTestConstants.SENTENCE_TWO;
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute);

        List<ITuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL);

        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest5ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 6:Test getNextTuple using two fields:
     * <p>
     * Sentence1: Microsoft, Google and Facebook are organizations. Sentence2:
     * Donald Trump and Barack Obama are persons.
     * <p>
     * Only search for Organization for all fields.
     */
    @Test
    public void getNextTupleTest6() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest4Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(TWO_SENTENCE_TABLE, tuple);
        }

        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        String attribute2 = NlpExtractorTestConstants.SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<ITuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames,
                NlpPredicate.NlpTokenType.Organization);

        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest6ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    /**
     * Scenario 7:Test getNextTuple using sentence: Sentence1: Feeling the warm
     * sun rays beaming steadily down, the girl decided there was no need to
     * wear a coat. Search for Adjective.
     */
    @Test
    public void getNextTupleTest7() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest7Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }

        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.Adjective);

        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest7ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }

    @Test
    public void getNextTupleTest8() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest8Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }

        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.Money);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest8ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    @Test
    public void getNextTupleTest9() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTest9Tuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(TWO_SENTENCE_TABLE, tuple);
        }

        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        String attribute2 = NlpExtractorTestConstants.SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<ITuple> returnedResults = Utils.removeFields(
                getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL), SchemaConstants.PAYLOAD);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest9ResultTuples();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void getNextTupleTest10() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getOneSentenceTestTuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }
        
        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        List<String> attributeNames = Arrays.asList(attribute1);
        
        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest10ResultTuples();
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    @Test
    public void getNextTupleTest11() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getTwoSentenceTestTuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(TWO_SENTENCE_TABLE, tuple);
        }
        
        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        String attribute2 = NlpExtractorTestConstants.SENTENCE_TWO;
        List<String> attributeNames = Arrays.asList(attribute1, attribute2);
        
        List<ITuple> returnedResults = Utils.removeFields(
                getQueryResults(TWO_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL), SchemaConstants.PAYLOAD);
        
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest11ResultTuple();  
        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
    
    public void getNextTupleTestWithLimit() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getOneSentenceTestTuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }
        
        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        List<String> attributeNames = Arrays.asList(attribute1);
        
        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL, 3, 0);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest10ResultTuples();
        
        // ExpectedResults is the array containing all the matches.
        // Since the order of returning records in returnedResults is not deterministic, we use containsAll
        // to ensure that the records in returnedResults are included in the ExpectedResults.
        Assert.assertEquals(returnedResults.size(), 3);
        Assert.assertTrue(TestUtils.containsAll(expectedResults, returnedResults));
    }
    
    public void getNextTupleTestWithLimitOffset() throws Exception {
        List<ITuple> data = NlpExtractorTestConstants.getOneSentenceTestTuple();
        for (ITuple tuple : data) {
            RelationManager.getRelationManager().insertTuple(ONE_SENTENCE_TABLE, tuple);
        }
        
        String attribute1 = NlpExtractorTestConstants.SENTENCE_ONE;
        List<String> attributeNames = Arrays.asList(attribute1);
        
        List<ITuple> returnedResults = getQueryResults(ONE_SENTENCE_TABLE, attributeNames, NlpPredicate.NlpTokenType.NE_ALL, 2, 2);
        List<ITuple> expectedResults = NlpExtractorTestConstants.getTest10ResultTuples();
        
        Assert.assertEquals(returnedResults.size(), 2);
        Assert.assertTrue(TestUtils.containsAll(expectedResults, returnedResults));
    }

}
