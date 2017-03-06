package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.StudentTestP2;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runners.MethodSorters;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class TestLargeBPlusTree {
    public static final String testFile = "BPlusTreeTest";
    private BPlusTree bp;
    public static final int intLeafPageSize = 400;
    public static final int intInnPageSize = 496;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    // 160 seconds max per method tested.
    public Timeout globalTimeout = Timeout.seconds(160);

    @Before
    public void beforeEach() throws Exception {
        tempFolder.newFile(testFile);
        String tempFolderPath = tempFolder.getRoot().getAbsolutePath();
        this.bp = new BPlusTree(new IntDataBox(), testFile, tempFolderPath);
    }

    /**     
    * Test sample, do not modify.        
    */
    @Test
    @Category(StudentTestP2.class)
    public void testSample() {
        assertEquals(true, true); // Do not actually write a test like this!
    }

    @Test
    public void testBPlusTreeInsert() {
        /** Insert records to separate pages in increasing pageNum order. */
        for (int i = 0; i < 10; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i,0));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        int expectedPageNum = 0;
        while (rids.hasNext()) {
            assertEquals(expectedPageNum, rids.next().getPageNum());
            expectedPageNum++;
        }
        assertEquals(1, this.bp.getNumNodes());
    }

    @Test
    public void testBPlusTreeInsertIterateFrom() {
        /**
         * Scan records starting from the middle page after inserting records
         * to separate pages in decreasing pageNum order.
         */
        for (int i = 16; i >= 0; i--) {
            bp.insertKey(new IntDataBox(i), new RecordID(i,0));
        }
        Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataBox(10));
        int expectedPageNum = 10;
        while (rids.hasNext()) {
            assertEquals(expectedPageNum, rids.next().getPageNum());
            expectedPageNum++;
        }
        assertEquals(17, expectedPageNum);
        assertEquals(1, this.bp.getNumNodes());
    }

    @Test
    public void testBPlusTreeInsertIterateFullLeafNode() {
        /** Insert enough records to fill a leaf. */
        for (int i = 0; i < intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i,0));
        }
        Iterator<RecordID> rids = bp.sortedScan();
        int expectedPageNum = 0;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedPageNum, rid.getPageNum());
            expectedPageNum++;
        }
        assertEquals(intLeafPageSize, expectedPageNum);
        assertEquals(1, this.bp.getNumNodes());
    }

    @Test
    public void testBPlusTreeInsertIterateFullLeafSplit() {
        /** Split a full leaf by inserting a leaf's page size + 1 records. */
        for (int i = 0; i < intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i,0));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        assertTrue(rids.hasNext());
        int expectedPageNum = 0;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedPageNum, rid.getPageNum());
            expectedPageNum++;
        }
        assertEquals(intLeafPageSize + 1, expectedPageNum);
        assertEquals(3, this.bp.getNumNodes());
    }

    @Test
    public void testBPlusTreeInsertAppendIterateMultipleFullLeafSplit() {
        /**
         * Split leaves three times by inserting enough records for four
         * leaves: three full and one with a record.
         */
        for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i,0));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        int expectedPageNum = 0;
        while (rids.hasNext()) {
            RecordID rid = rids.next();
            assertEquals(expectedPageNum, rid.getPageNum());
            expectedPageNum++;
        }
        assertEquals(3*intLeafPageSize + 1, expectedPageNum);
        assertEquals(7, this.bp.getNumNodes());
    }


    @Test
    public void testFullPage() {
        /**
         * Insert four leaves in a sweeping fashion: three full and one with a
         * record.
         */
        for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i % 3), new RecordID(i % 3, i));
        }

        assertEquals(5, this.bp.getNumNodes());

        Iterator<RecordID> rids = bp.sortedScan();
        for (int i = 0; i < intLeafPageSize + 1; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
    }

    @Test
    public void testBPlusTreeSweepInsertSortedScanMultipleFullLeafSplit() {
        /**
         * Insert 3 full leafs of records plus one additional record.
         * Inserts are done in a sweeping fashion:
         *   1st insert of value 0 on (page 0, entry 0)
         *   2nd insert of value 1 on (page 1, entry 1)
         *   3rd insert of value 2 on (page 2, entry 2)
         *   4th insert of value 0 on (page 0, entry 4) ...
         * Expect page number and key number to be the same.
         */
        for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
            bp.insertKey(new IntDataBox(i % 3), new RecordID(i % 3, i));
        }

        assertEquals(5, this.bp.getNumNodes());

        Iterator<RecordID> rids = bp.sortedScan();
        assertTrue(rids.hasNext());

        for (int i = 0; i < intLeafPageSize + 1; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
            assertEquals(i * 3, rid.getEntryNumber());
        }

        for (int i = 0; i < intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(1, rid.getPageNum());
        }

        for (int i = 0; i < intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(2, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    public void testBPlusTreeSweepInsertLookupKeyMultipleFullLeafSplit() {
        /**
         * Insert four full leaves of records in a sweeping fashion.
         * Ensure that you correctly handle multiple leaf splits.
         */
        for (int i = 0; i < 8*intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 4), new RecordID(i % 4, i));
        }

        assertEquals(15, this.bp.getNumNodes());

        Iterator<RecordID> rids;

        rids = bp.lookupKey(new IntDataBox(0));
        System.out.println(rids);
        assertTrue(rids.hasNext());

        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue("iteration " + i, rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.lookupKey(new IntDataBox(1));
        assertTrue(rids.hasNext());
        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(1, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.lookupKey(new IntDataBox(2));
        assertTrue(rids.hasNext());

        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(2, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.lookupKey(new IntDataBox(3));
        assertTrue(rids.hasNext());

        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(3, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

    }

    @Test
    public void testBPlusTreeSweepInsertSortedScanLeafSplit() {
        /**
         * Insert ten full leaves of records in a sweeping fashion.
         * Ensure that iterator works when a value spans more than one page.
         * Example: Two leaf pages will contain keys with a value of 0.
         */
        for (int i = 0; i < 10*intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 5), new RecordID(i % 5, i));
        }

        assertEquals(19, this.bp.getNumNodes());

        Iterator<RecordID> rids = bp.sortedScan();
        assertTrue(rids.hasNext());
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 2*intLeafPageSize; j++) {
                assertTrue(rids.hasNext());
                RecordID rid = rids.next();
                assertEquals(i, rid.getPageNum());
            }
        }
        assertFalse(rids.hasNext());

    }

    @Test
    public void testBPlusTreeSweepInsertSortedScanFromLeafSplit() {
        /**
         * Insert ten full leaves of records in a sweeping fashion.
         * Ensure that sortedScanFrom works when a value spans more than one
         * page.
         */
        for (int i = 0; i < 10*intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 5), new RecordID(i % 5, i));
        }

        assertEquals(19, this.bp.getNumNodes());

        for (int k = 0; k < 5; k++) {
            Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataBox(k));
            assertTrue(rids.hasNext());
            for (int i = k; i < 5; i++) {
                for (int j = 0; j < 2*intLeafPageSize; j++) {
                    assertTrue(rids.hasNext());
                    RecordID rid = rids.next();
                    assertEquals(i, rid.getPageNum());
                }
            }
            assertFalse(rids.hasNext());
        }
    }

    @Test
    public void testBPlusTreeAppendInsertSortedScanInnerSplit() {
        /**
         * Insert enough keys to cause an InnerNode split.
         */
        for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i, 0));
        }

        assertEquals(498, this.bp.getNumNodes());

        Iterator<RecordID> rids = bp.sortedScan();

        for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(i, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

    }

    @Test
    public void testBPlusTreeSweepInsertLookupInnerSplit() {
        /**
         * Insert enough keys to cause an InnerNode split: numEntries +
         * firstChild.
         * Each key should span 2 pages.
         */
        for (int i = 0; i < 2*intLeafPageSize; i++) {
            for (int k = 0; k < 250; k++) {
                bp.insertKey(new IntDataBox(k), new RecordID(k, 0));
            }
        }

        assertEquals(865, this.bp.getNumNodes());

        for (int k = 0; k < 250; k++) {
            Iterator<RecordID> rids = bp.lookupKey(new IntDataBox(k));
            for (int i = 0; i < 2*intLeafPageSize; i++) {
                assertTrue("Loop: " + k + " iteration " + i, rids.hasNext());
                RecordID rid = rids.next();
                assertEquals(k, rid.getPageNum());
            }
            assertFalse(rids.hasNext());
        }
    }

    /* ************** 10 student tests ************** */
    @Test
    @Category(StudentTestP2.class)
    public void test_1_InsertAllIdenticalKeys() {
        for (int i = 0; i < 2*intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(0), new RecordID(0, 0));
        }
        assertEquals(4, this.bp.getNumNodes());

        Iterator<RecordID> rids = bp.sortedScan();

        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.lookupKey(new IntDataBox(0));
        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.sortedScanFrom(new IntDataBox(0));
        for (int i = 0; i < 2*intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_2_InsertAllIdenticalKeysToKeepTrackSplit() {
        for (int i = 0; i < intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(0), new RecordID(0, 0));
        }
        assertEquals(1, this.bp.getNumNodes());

        bp.insertKey(new IntDataBox(0), new RecordID(0, 0));
        assertEquals(3, this.bp.getNumNodes());

        for (int i = 0; i < intLeafPageSize / 2 - 1; i++) {
            bp.insertKey(new IntDataBox(0), new RecordID(0, 0));
        }
        assertEquals(3, this.bp.getNumNodes());
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_3_InsertKeysToSplitRoot() {
        for (int i = 0; i < intLeafPageSize / 2; i++) {
            bp.insertKey(new IntDataBox(0), new RecordID(0, 0));
        }

        for (int i = 0; i < intLeafPageSize / 2; i++) {
            bp.insertKey(new IntDataBox(1), new RecordID(0, 0));
        }

        bp.insertKey(new IntDataBox(0), new RecordID(0, 0));

        Iterator<RecordID> rids = bp.lookupKey(new IntDataBox(0));
        for (int i = 0; i < intLeafPageSize / 2 + 1; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(0, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        List<BEntry> allEntries_1 = BPlusNode.getBPlusNode(bp, 1).getAllValidEntries();
        List<BEntry> allEntries_2 = BPlusNode.getBPlusNode(bp, 2).getAllValidEntries();

        for (BEntry entry : allEntries_1) {
            DataBox box = new IntDataBox(0);
            assertEquals(box, entry.getKey());
        }

        for (int i = 0; i < intLeafPageSize / 2 + 1; i++) {
            BEntry entry = allEntries_2.get(i);
            DataBox box_0 = new IntDataBox(0);
            DataBox box_1 = new IntDataBox(1);
            if (i == 0) {
                assertEquals(box_0, entry.getKey());
            } else {
                assertEquals(box_1, entry.getKey());
            }
        }
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_4_InsertKeysToTestRootAndLeafNode() {
        for (int i = 0; i < 16 * intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 16), new RecordID(i % 16, 0));
        }

        Iterator<RecordID> rids = bp.lookupKey(new IntDataBox(4));
        for (int i = 0; i < intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(4, rid.getPageNum());
        }
        assertFalse(rids.hasNext());

        rids = bp.sortedScanFrom(new IntDataBox(8));
        for (int i = 0; i < 8 * intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(8 + i / intLeafPageSize, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_5_InsertDecreasingKeys() {
        for (int i = 0; i < 10 * intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(10 * intLeafPageSize - i), new RecordID(i, 0));
        }

        Iterator<RecordID> rids = bp.lookupKey(new IntDataBox(5 * intLeafPageSize));
        assertTrue(rids.hasNext());
        RecordID rid = rids.next();
        assertEquals(5 * intLeafPageSize, rid.getPageNum());
        assertFalse(rids.hasNext());

        rids = bp.sortedScan();
        for (int i = 0; i < 10 * intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            rid = rids.next();
            assertEquals(10 * intLeafPageSize - i - 1, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_6_InsertLeapFrogKeysForLookup() {
        for (int i = 0; i < 16 * intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 16), new RecordID(i % 16, 0));
        }
        Iterator<RecordID> rids;
        rids = bp.lookupKey(new IntDataBox(16));
        assertFalse(rids.hasNext());

        for (int k = 0; k < 16; k ++) {
            rids = bp.lookupKey(new IntDataBox(k));
            for (int i = 0; i < intLeafPageSize; i++) {
                assertTrue(rids.hasNext());
                RecordID rid = rids.next();
                assertEquals(k, rid.getPageNum());
            }
            assertFalse(rids.hasNext());
        }
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_7_InsertKeysInIncreasingOrder() {
        for (int i = 0; i < 8 * intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i), new RecordID(i, 0));
        }
        Iterator<RecordID> rids;

        rids = bp.sortedScanFrom(new IntDataBox(4 * intLeafPageSize));
        for (int i = 0; i < 4 * intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(i + 4 * intLeafPageSize, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_8_InsertLeapFrogKeysForScanFrom() {
        for (int i = 0; i < 8 * intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 8), new RecordID(i % 8, 0));
        }

        Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataBox(4));
        for (int i = 0; i < 4 * intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(4 + i / intLeafPageSize, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_9_InsertZeroAndOneKeyToCheckPageNum() {
        for (int i = 0; i < 4 * intLeafPageSize; i++) {
            bp.insertKey(new IntDataBox(i % 2), new RecordID(i, 0));
        }

        Iterator<RecordID> rids = bp.sortedScan();
        for (int i = 0; i < 2 * intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(2 * i, rid.getPageNum());
        }
        for (int i = 0; i < 2 * intLeafPageSize; i++) {
            assertTrue(rids.hasNext());
            RecordID rid = rids.next();
            assertEquals(2 * i + 1, rid.getPageNum());
        }
        assertFalse(rids.hasNext());
    }

    @Test
    @Category(StudentTestP2.class)
    public void test_10_InsertOneFinalKey() {
        bp.insertKey(new IntDataBox(999), new RecordID(999, 0));

        Iterator<RecordID> rids;

        rids = bp.sortedScanFrom(new IntDataBox(0));
        assertTrue(rids.hasNext());
        RecordID rid = rids.next();
        assertEquals(999, rid.getPageNum());
        assertFalse(rids.hasNext());
    }
}


